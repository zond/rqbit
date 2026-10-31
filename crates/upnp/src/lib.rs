use anyhow::{Context, bail};
use bstr::BStr;
use futures::{StreamExt, TryFutureExt, stream::FuturesUnordered};
use librqbit_dualstack_sockets::{BindDevice, UdpSocket};
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        watch,
    },
    time::Instant,
};
use tracing::{Instrument, Span, debug, debug_span, info, trace, warn};
use url::Url;

pub mod http_client;

pub use http_client::http_client_builder;

const SERVICE_TYPE_WAN_IP_CONNECTION: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";
/// What routers with a PPPoE WAN, such as most DSL ones, offer instead of
/// WANIPConnection. Its AddPortMapping is the same.
const SERVICE_TYPE_WAN_PPP_CONNECTION: &str = "urn:schemas-upnp-org:service:WANPPPConnection:1";
const SSDP_MULTICAST_IP: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900));
pub const SSDP_SEARCH_WAN_IPCONNECTION_ST: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";
pub const SSDP_SEARCH_WAN_PPPCONNECTION_ST: &str =
    "urn:schemas-upnp-org:service:WANPPPConnection:1";
/// What the port forwarder searches for: either WAN connection service.
const WAN_CONNECTION_SEARCH_TARGETS: [&str; 2] = [
    SSDP_SEARCH_WAN_IPCONNECTION_ST,
    SSDP_SEARCH_WAN_PPPCONNECTION_ST,
];
pub const SSDP_SEARCH_ROOT_ST: &str = "upnp:rootdevice";

pub fn make_ssdp_search_request(kind: &str) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\n\
            Host: 239.255.255.250:1900\r\n\
            Man: \"ssdp:discover\"\r\n\
            MX: 3\r\n\
            ST: {kind}\r\n\
            \r\n"
    )
}

pub fn get_local_ip_relative_to(
    local_dest: SocketAddr,
    interfaces: &[NetworkInterface],
) -> anyhow::Result<IpAddr> {
    fn masked_v4(ip: Ipv4Addr, mask: Ipv4Addr) -> u32 {
        ip.to_bits() & mask.to_bits()
    }

    fn masked_v6(ip: Ipv6Addr, mask: Ipv6Addr) -> u128 {
        ip.to_bits() & mask.to_bits()
    }

    for i in interfaces {
        for addr in i.addr.iter() {
            match (local_dest, addr.ip(), addr.netmask()) {
                // We are connecting to ourselves, return itself.
                (l, a, _) if l.ip() == a => return Ok(addr.ip()),
                // IPv4 masks match.
                (SocketAddr::V4(l), IpAddr::V4(a), Some(IpAddr::V4(m)))
                    if masked_v4(*l.ip(), m) == masked_v4(a, m) =>
                {
                    return Ok(addr.ip());
                }
                // Return IPv6 link-local addresses when source is link-local address and there's a scope_id set.
                (SocketAddr::V6(l), IpAddr::V6(a), _)
                    if l.ip().is_unicast_link_local() && l.scope_id() > 0 =>
                {
                    if a.is_unicast_link_local() && l.scope_id() == i.index {
                        return Ok(addr.ip());
                    }
                }
                // If V6 masks match, return.
                (SocketAddr::V6(l), IpAddr::V6(a), Some(IpAddr::V6(m)))
                    if masked_v6(*l.ip(), m) == masked_v6(a, m) =>
                {
                    return Ok(addr.ip());
                }
                // For IPv6 fallback to returning a random (first encountered) IPv6 address.
                (SocketAddr::V6(_), IpAddr::V6(_), None) => return Ok(addr.ip()),
                _ => continue,
            }
        }
    }
    bail!("couldn't find a local ip address")
}

async fn forward_port(
    service: &WanService,
    local_ip: IpAddr,
    port: u16,
    lease_duration: Duration,
) -> anyhow::Result<()> {
    let request_body = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
            <s:Body>
                <u:AddPortMapping xmlns:u="{service_type}">
                    <NewRemoteHost></NewRemoteHost>
                    <NewExternalPort>{port}</NewExternalPort>
                    <NewProtocol>TCP</NewProtocol>
                    <NewInternalPort>{port}</NewInternalPort>
                    <NewInternalClient>{local_ip}</NewInternalClient>
                    <NewEnabled>1</NewEnabled>
                    <NewPortMappingDescription>rust UPnP</NewPortMappingDescription>
                    <NewLeaseDuration>{}</NewLeaseDuration>
                </u:AddPortMapping>
            </s:Body>
        </s:Envelope>
    "#,
        lease_duration.as_secs(),
        service_type = service.service_type,
    );

    let url = service.control_url.clone();

    let client = http_client::http_client_builder()
        .build()
        .context("error building HTTP client")?;
    let response = client
        .post(url.clone())
        .header("Content-Type", "text/xml")
        .header(
            "SOAPAction",
            format!("\"{}#AddPortMapping\"", service.service_type),
        )
        .body(request_body)
        .send()
        .await
        .context("error sending")?;

    let status = response.status();

    let response_text = response
        .text()
        .await
        .context("error reading response text")?;

    trace!(status = %status, text=response_text, "AddPortMapping response");
    if !status.is_success() {
        bail!("failed port forwarding: {}", status);
    } else {
        debug!(%local_ip, port, "successfully port forwarded");
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RootDesc {
    #[serde(rename = "device")]
    pub devices: Vec<Device>,
}

#[derive(Default, Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeviceList {
    #[serde(rename = "device")]
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Device {
    #[serde(rename = "deviceType")]
    pub device_type: String,
    #[serde(rename = "friendlyName", default)]
    pub friendly_name: String,
    #[serde(rename = "serviceList", default)]
    pub service_list: ServiceList,
    #[serde(rename = "deviceList", default)]
    pub device_list: DeviceList,
}

impl Device {
    pub fn iter_services(
        &self,
        parent: Span,
    ) -> Box<dyn Iterator<Item = (tracing::Span, &Service)> + '_> {
        let self_span = self.span(parent);
        let services = self.service_list.services.iter().map({
            let self_span = self_span.clone();
            move |s| (s.span(self_span.clone()), s)
        });
        Box::new(services.chain(self.device_list.devices.iter().flat_map({
            let self_span = self_span.clone();
            move |d| d.iter_services(self_span.clone())
        })))
    }

    pub fn span(&self, parent: tracing::Span) -> tracing::Span {
        debug_span!(parent: parent, "device", device = self.name())
    }
}

impl Device {
    pub fn name(&self) -> &str {
        if self.friendly_name.is_empty() {
            return &self.device_type;
        }
        &self.friendly_name
    }
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct ServiceList {
    #[serde(rename = "service", default)]
    pub services: Vec<Service>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Service {
    #[serde(rename = "serviceType")]
    pub service_type: String,
    #[serde(rename = "controlURL")]
    pub control_url: String,
    #[serde(rename = "SCPDURL")]
    pub scpd_url: String,
    #[serde(rename = "eventSubURL", default)]
    pub event_sub_url: Option<String>,
}

impl Service {
    pub fn span(&self, parent: tracing::Span) -> tracing::Span {
        debug_span!(parent: parent, "service", url = self.control_url)
    }
}

#[derive(Debug)]
struct UpnpEndpoint {
    discover_response: UpnpDiscoverResponse,
    data: RootDesc,
    nics: Vec<NetworkInterface>,
}

impl UpnpEndpoint {
    fn span(&self) -> tracing::Span {
        debug_span!("upnp_endpoint", location = %self.discover_response.location)
    }

    fn iter_services(&self) -> impl Iterator<Item = (tracing::Span, &Service)> + '_ {
        let self_span = self.span();
        self.data
            .devices
            .iter()
            .flat_map(move |d| d.iter_services(self_span.clone()))
    }

    fn my_local_ip(&self) -> anyhow::Result<IpAddr> {
        let received_from = self.discover_response.received_from;
        let local_ip = get_local_ip_relative_to(received_from, &self.nics)
            .with_context(|| format!("can't determine local IP relative to {received_from}"))?;
        Ok(local_ip)
    }

    fn get_wan_services(&self) -> impl Iterator<Item = WanService> + '_ {
        self.iter_services()
            .filter_map(|(_, s)| {
                let service_type = [
                    SERVICE_TYPE_WAN_IP_CONNECTION,
                    SERVICE_TYPE_WAN_PPP_CONNECTION,
                ]
                .into_iter()
                .find(|t| *t == s.service_type)?;
                Some((
                    service_type,
                    self.discover_response.location.join(&s.control_url),
                ))
            })
            .filter_map(|(service_type, url)| match url {
                Ok(control_url) => Some(WanService {
                    control_url,
                    service_type,
                }),
                Err(e) => {
                    debug!("bad control url: {e:#}");
                    None
                }
            })
    }
}

#[derive(Debug)]
pub struct UpnpDiscoverResponse {
    pub received_from: SocketAddr,
    pub location: Url,
}

pub async fn discover_services(location: Url) -> anyhow::Result<RootDesc> {
    let response = http_client::http_client_builder()
        .build()
        .context("error building HTTP client")?
        .get(location.clone())
        .send()
        .await
        .context("failed to send GET request")?
        .text()
        .await
        .context("failed to read response body")?;
    trace!("received from {location}: {response}");
    let root_desc: RootDesc = quick_xml::de::from_str(&response)
        .context("failed to parse response body as xml")
        .inspect_err(|e| {
            debug!("failed to parse this XML: {response}. Error: {e:#}");
        })?;
    Ok(root_desc)
}

pub fn parse_upnp_discover_response(
    buf: &[u8],
    received_from: SocketAddr,
) -> anyhow::Result<UpnpDiscoverResponse> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut resp = httparse::Response::new(&mut headers);
    resp.parse(buf).context("error parsing response")?;

    trace!(?resp, "parsed SSDP response");
    match resp.code {
        Some(200) => {}
        other => anyhow::bail!("bad response code {other:?}, expected 200"),
    }
    let mut location = None;
    for header in resp.headers {
        match header.name {
            "location" | "LOCATION" | "Location" => {
                location = Some(
                    std::str::from_utf8(header.value).context("bad utf-8 in location header")?,
                )
            }
            _ => continue,
        }
    }
    let location = location.context("missing location header")?;
    let location =
        Url::parse(location).with_context(|| format!("failed parsing location {location}"))?;
    Ok(UpnpDiscoverResponse {
        location,
        received_from,
    })
}

pub async fn discover_once(
    tx: &UnboundedSender<UpnpDiscoverResponse>,
    kind: &str,
    timeout: Duration,
    bind_device: Option<&BindDevice>,
) -> anyhow::Result<()> {
    search(tx, &[kind], timeout, bind_device, SSDP_MULTICAST_IP)
        .await
        .map(|_| ())
}

/// One SSDP search for each of `kinds`, from one socket, sent to `to` (the
/// SSDP multicast group but in tests): what [`discover_once`] does,
/// returning how many endpoints answered.
async fn search(
    tx: &UnboundedSender<UpnpDiscoverResponse>,
    kinds: &[&str],
    timeout: Duration,
    bind_device: Option<&BindDevice>,
    to: SocketAddr,
) -> anyhow::Result<usize> {
    // TODO: do we need IPv6 support here? I can't test it, don't have the hardware for it (router / IPv6 provider).
    let socket = UdpSocket::bind_udp(
        (Ipv4Addr::UNSPECIFIED, 0).into(),
        librqbit_dualstack_sockets::BindOpts {
            device: bind_device,
            ..Default::default()
        },
    )?;

    for kind in kinds {
        let message = make_ssdp_search_request(kind);
        socket
            .send_to(message.as_bytes(), to)
            .await
            .with_context(|| format!("failed to send SSDP search request to {to}"))?;
    }

    let mut buffer = [0; 2048];

    let timeout = tokio::time::sleep(timeout);
    let mut timed_out = false;
    tokio::pin!(timeout);

    let mut discovered = 0;

    while !timed_out {
        tokio::select! {
            _ = &mut timeout, if !timed_out => {
                timed_out = true;
            }
            Ok((len, addr)) = socket.recv_from(&mut buffer), if !timed_out => {
                let response = &buffer[..len];
                match parse_upnp_discover_response(response, addr) {
                    Ok(r) => {
                        tx.send(r)?;
                        discovered += 1;
                    },
                    // A device that answers the search with something that is
                    // not a gateway's reply does so every search; a line a
                    // minute about it is noise, and the state already says
                    // whether a gateway was found.
                    Err(e) => debug!(response=?BStr::new(response), "failed to parse SSDP response: {e:#}"),
                };
            },
        }
    }

    debug!("discovered {discovered} endpoints");
    Ok(discovered)
}

pub struct UpnpPortForwarderOptions {
    pub lease_duration: Duration,
    /// How often to search for gateways while one answers.
    pub discover_interval: Duration,
    pub discover_timeout: Duration,
    /// The longest wait between searches while none answers. The wait starts
    /// at `discover_interval` and doubles after each search that finds
    /// nothing or fails, up to this; it drops back to `discover_interval` once
    /// a gateway answers or a mapping is lost.
    pub discover_interval_max: Duration,
}

impl Default for UpnpPortForwarderOptions {
    fn default() -> Self {
        Self {
            discover_interval: Duration::from_secs(60),
            discover_timeout: Duration::from_secs(10),
            discover_interval_max: Duration::from_secs(600),
            lease_duration: Duration::from_secs(60),
        }
    }
}

/// What a [`UpnpPortForwarder`] knows about the network and its mappings,
/// published on the channel [`UpnpPortForwarder::subscribe`] returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PortForwardingState {
    /// Whether an SSDP search has finished yet, whatever it found. Until one
    /// has, no gateway is not news.
    pub searched: bool,
    /// The gateway that answered the latest search, once its description has
    /// been read and it offers a WAN connection service. A search nothing
    /// answers, or one that fails, clears it.
    pub gateway: Option<PortForwardingGateway>,
    /// Why the latest search failed, if it did. A search nobody answers has
    /// not failed: that is `gateway` being empty.
    pub discovery_error: Option<String>,
    /// One entry per port the forwarder was asked to map, in that order.
    pub ports: Vec<PortMappingState>,
}

/// A UPnP gateway that answered the search.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PortForwardingGateway {
    /// The device's friendly name, or its device type if it has none.
    pub name: String,
    /// Where its description lives, as the gateway announced it.
    pub location: String,
}

/// One port the forwarder maps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PortMappingState {
    pub port: u16,
    /// Whether a gateway holds a mapping for this port right now: an
    /// AddPortMapping succeeded and its lease has not run out since. A
    /// mapping that fails to renew stops being held when its lease ends.
    pub held: bool,
    /// The control URLs of the gateway services holding the mapping; empty
    /// exactly when `held` is false.
    pub held_via: Vec<String>,
    /// When an AddPortMapping for this port last succeeded.
    pub mapped_at: Option<SystemTime>,
    /// Why the latest AddPortMapping for this port failed, until one
    /// succeeds.
    pub error: Option<String>,
}

impl PortForwardingState {
    fn new(ports: &[u16]) -> Self {
        Self {
            searched: false,
            gateway: None,
            discovery_error: None,
            ports: ports
                .iter()
                .map(|&port| PortMappingState {
                    port,
                    held: false,
                    held_via: Vec::new(),
                    mapped_at: None,
                    error: None,
                })
                .collect(),
        }
    }

    /// The entry for `port`, if the forwarder maps it.
    pub fn port(&self, port: u16) -> Option<&PortMappingState> {
        self.ports.iter().find(|p| p.port == port)
    }

    fn port_mut(&mut self, port: u16) -> Option<&mut PortMappingState> {
        self.ports.iter_mut().find(|p| p.port == port)
    }
}

/// Changes the published state, waking subscribers only if it changed.
fn update_state(
    state: &watch::Sender<PortForwardingState>,
    f: impl FnOnce(&mut PortForwardingState),
) {
    state.send_if_modified(|s| {
        let before = s.clone();
        f(s);
        *s != before
    });
}

/// A WAN connection service on a gateway: where to send AddPortMapping, and
/// the service type the SOAP action must name.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WanService {
    control_url: Url,
    service_type: &'static str,
}

/// A gateway's description, read after it answered a search.
struct FoundGateway {
    name: String,
    location: Url,
    services: Vec<WanService>,
    /// Our address on the gateway's network, which the mapping points at.
    local_ip: anyhow::Result<IpAddr>,
}

/// The network as the forwarder's loops see it, so that tests can stand in
/// for a router.
trait Gateways {
    /// Searches for gateways, sending each answer to `found`, and returns how
    /// many answered.
    async fn search(&self, found: &UnboundedSender<UpnpDiscoverResponse>) -> anyhow::Result<usize>;

    /// Reads the description of a gateway that answered.
    async fn describe(&self, response: UpnpDiscoverResponse) -> anyhow::Result<FoundGateway>;

    async fn add_port_mapping(
        &self,
        service: &WanService,
        local_ip: IpAddr,
        port: u16,
        lease_duration: Duration,
    ) -> anyhow::Result<()>;
}

/// The real network: SSDP, HTTP and SOAP.
struct NetworkGateways {
    bind_device: Option<BindDevice>,
    discover_timeout: Duration,
}

impl Gateways for NetworkGateways {
    async fn search(&self, found: &UnboundedSender<UpnpDiscoverResponse>) -> anyhow::Result<usize> {
        search(
            found,
            &WAN_CONNECTION_SEARCH_TARGETS,
            self.discover_timeout,
            self.bind_device.as_ref(),
            SSDP_MULTICAST_IP,
        )
        .await
    }

    async fn describe(&self, response: UpnpDiscoverResponse) -> anyhow::Result<FoundGateway> {
        let location = response.location.clone();
        let data = discover_services(location.clone()).await?;
        let nics = network_interface::NetworkInterface::show()
            .context("error listing network interfaces")?;
        let endpoint = UpnpEndpoint {
            discover_response: response,
            data,
            nics,
        };
        Ok(FoundGateway {
            name: endpoint
                .data
                .devices
                .first()
                .map(|d| d.name().to_owned())
                .unwrap_or_default(),
            location,
            services: endpoint.get_wan_services().collect(),
            local_ip: endpoint.my_local_ip(),
        })
    }

    async fn add_port_mapping(
        &self,
        service: &WanService,
        local_ip: IpAddr,
        port: u16,
        lease_duration: Duration,
    ) -> anyhow::Result<()> {
        forward_port(service, local_ip, port, lease_duration).await
    }
}

pub struct UpnpPortForwarder {
    ports: Vec<u16>,
    opts: UpnpPortForwarderOptions,
    bind_device: Option<BindDevice>,
    state: watch::Sender<PortForwardingState>,
}

impl UpnpPortForwarder {
    pub fn new(
        ports: Vec<u16>,
        opts: Option<UpnpPortForwarderOptions>,
        bind_device: Option<BindDevice>,
    ) -> anyhow::Result<Self> {
        if ports.is_empty() {
            bail!("empty ports")
        }
        let state = watch::Sender::new(PortForwardingState::new(&ports));
        Ok(Self {
            ports,
            opts: opts.unwrap_or_default(),
            bind_device,
            state,
        })
    }

    /// The forwarder's state as it changes. Subscribe before
    /// [`Self::run_forever`] consumes the forwarder; receivers keep working
    /// while it runs.
    pub fn subscribe(&self) -> watch::Receiver<PortForwardingState> {
        self.state.subscribe()
    }

    pub async fn run_forever(self) -> ! {
        let gateways = NetworkGateways {
            bind_device: self.bind_device,
            discover_timeout: self.opts.discover_timeout,
        };
        run(&self.ports, &self.opts, &gateways, &self.state).await
    }
}

/// A failure that can last, such as no gateway on the network: warned about
/// once when it starts, logged at debug while it lasts, and noted at info
/// when it clears, so a network without UPnP costs one warning, not one a
/// minute.
#[derive(Default)]
struct Standing {
    failing: bool,
}

impl Standing {
    fn fail(&mut self, what: std::fmt::Arguments<'_>) {
        if std::mem::replace(&mut self.failing, true) {
            debug!("{what}");
        } else {
            warn!("{what}");
        }
    }

    fn clear(&mut self, what: std::fmt::Arguments<'_>) {
        if std::mem::take(&mut self.failing) {
            info!("{what}");
        }
    }
}

async fn discovery<G: Gateways>(
    gateways: &G,
    opts: &UpnpPortForwarderOptions,
    tx: UnboundedSender<UpnpDiscoverResponse>,
    mut mapping_lost: UnboundedReceiver<()>,
    state: &watch::Sender<PortForwardingState>,
) {
    let base = opts.discover_interval;
    let max = opts.discover_interval_max.max(base);
    // The wait after the next search that finds nothing.
    let mut backoff = base;
    let mut search_failed = Standing::default();
    let mut no_gateway = Standing::default();

    loop {
        let started = Instant::now();
        let result = gateways.search(&tx).await;
        update_state(state, |s| {
            s.searched = true;
            match &result {
                Ok(0) => {
                    s.gateway = None;
                    s.discovery_error = None;
                }
                Ok(_) => s.discovery_error = None,
                Err(e) => {
                    s.gateway = None;
                    s.discovery_error = Some(format!("{e:#}"));
                }
            }
        });
        if result.is_ok() {
            search_failed.clear(format_args!("SSDP/UPNP discovery works again"));
        }
        let found = match result {
            Ok(0) => {
                no_gateway.fail(format_args!(
                    "no UPnP gateway answered; searching again in {backoff:?}"
                ));
                false
            }
            Ok(_) => {
                no_gateway.clear(format_args!("a UPnP gateway answered"));
                true
            }
            Err(e) => {
                search_failed.fail(format_args!(
                    "failed to run SSDP/UPNP discovery: {e:#}; searching again in {backoff:?}"
                ));
                false
            }
        };
        let wait = if found {
            backoff = base;
            base
        } else {
            let wait = backoff;
            backoff = (backoff * 2).min(max);
            wait
        };

        let mut next = started + wait;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(next) => break,
                Some(()) = mapping_lost.recv() => {
                    // The gateway stopped renewing: look for it, or its successor, soon.
                    backoff = base;
                    next = next.min(Instant::now() + base);
                }
            }
        }
    }
}

/// Resolves when the lease ending at `at` runs out; never, if none is held.
async fn lease_end(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

async fn manage_port<G: Gateways>(
    gateways: &G,
    service: &WanService,
    local_ip: IpAddr,
    port: u16,
    lease_duration: Duration,
    state: &watch::Sender<PortForwardingState>,
    mapping_lost: &UnboundedSender<()>,
) {
    let mut interval = tokio::time::interval(lease_duration / 2);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let via = service.control_url.to_string();
    // When the gateway drops the mapping unless we renew it first.
    let mut lease_ends: Option<Instant> = None;
    let lost = |lease_ends: &mut Option<Instant>| {
        *lease_ends = None;
        update_state(state, |s| {
            if let Some(p) = s.port_mut(port) {
                p.held_via.retain(|v| *v != via);
                p.held = !p.held_via.is_empty();
            }
        });
        let _ = mapping_lost.send(());
    };
    let mut refused = Standing::default();

    loop {
        tokio::select! {
            biased;
            _ = interval.tick() => {}
            _ = lease_end(lease_ends) => {
                lost(&mut lease_ends);
                continue;
            }
        }

        // The lease counts from when we asked, not from when the gateway answered.
        let sent = Instant::now();
        let attempt = gateways.add_port_mapping(service, local_ip, port, lease_duration);
        tokio::pin!(attempt);
        let result = loop {
            tokio::select! {
                biased;
                r = &mut attempt => break r,
                _ = lease_end(lease_ends) => lost(&mut lease_ends),
            }
        };

        match result {
            Ok(()) => {
                debug!(%local_ip, port, "successfully port forwarded");
                refused.clear(format_args!("port {port} is forwarded again"));
                lease_ends = Some(sent + lease_duration);
                update_state(state, |s| {
                    if let Some(p) = s.port_mut(port) {
                        if !p.held_via.contains(&via) {
                            p.held_via.push(via.clone());
                        }
                        p.held = true;
                        p.mapped_at = Some(SystemTime::now());
                        p.error = None;
                    }
                });
            }
            Err(e) => {
                refused.fail(format_args!("failed to forward port {port}: {e:#}"));
                update_state(state, |s| {
                    if let Some(p) = s.port_mut(port) {
                        p.error = Some(format!("{e:#}"));
                    }
                });
            }
        }
    }
}

async fn manage_service<G: Gateways>(
    gateways: &G,
    service: WanService,
    local_ip: IpAddr,
    ports: &[u16],
    opts: &UpnpPortForwarderOptions,
    state: &watch::Sender<PortForwardingState>,
    mapping_lost: &UnboundedSender<()>,
) {
    futures::future::join_all(ports.iter().map(|&port| {
        manage_port(
            gateways,
            &service,
            local_ip,
            port,
            opts.lease_duration,
            state,
            mapping_lost,
        )
        .instrument(debug_span!("manage_port", port = port))
    }))
    .await;
}

async fn run<G: Gateways>(
    ports: &[u16],
    opts: &UpnpPortForwarderOptions,
    gateways: &G,
    state: &watch::Sender<PortForwardingState>,
) -> ! {
    let (discover_tx, mut discover_rx) = unbounded_channel();
    let (mapping_lost_tx, mapping_lost_rx) = unbounded_channel();
    let discovery = discovery(gateways, opts, discover_tx, mapping_lost_rx, state);
    let mut no_local_ip = Standing::default();

    let mut spawned_tasks = HashSet::<Url>::new();

    let mut endpoints = FuturesUnordered::new();
    let mut service_managers = FuturesUnordered::new();

    tokio::pin!(discovery);

    loop {
        tokio::select! {
            _ = &mut discovery => {},
            r = discover_rx.recv() => {
                let r = r.unwrap();
                let location = r.location.clone();
                endpoints.push(gateways.describe(r).map_err(|e| {
                    debug!("error parsing endpoint: {e:#}");
                    e
                }).instrument(debug_span!("parse endpoint", location=location.to_string())));
            },
            Some(Ok(gateway)) = endpoints.next(), if !endpoints.is_empty() => {
                if gateway.services.is_empty() {
                    debug!(location = %gateway.location, "no WAN connection service");
                    continue;
                }
                update_state(state, |s| {
                    s.gateway = Some(PortForwardingGateway {
                        name: gateway.name.clone(),
                        location: gateway.location.to_string(),
                    })
                });
                for service in gateway.services {
                    if spawned_tasks.contains(&service.control_url) {
                        debug!("already spawned for {}", service.control_url);
                        continue;
                    }
                    let ip = match &gateway.local_ip {
                        Ok(ip) => {
                            no_local_ip.clear(format_args!("determined local IP {ip} for endpoint at {}", gateway.location));
                            *ip
                        },
                        Err(e) => {
                            no_local_ip.fail(format_args!("failed to determine local IP for endpoint at {}: {:#}", gateway.location, e));
                            break;
                        }
                    };
                    spawned_tasks.insert(service.control_url.clone());
                    let span = debug_span!("manage_service", url = %service.control_url);
                    service_managers.push(
                        manage_service(gateways, service, ip, ports, opts, state, &mapping_lost_tx)
                            .instrument(span),
                    );
                }
            },
            _ = service_managers.next(), if !service_managers.is_empty() => {

            },
        }
    }
}

#[cfg(test)]
mod tests {
    use quick_xml::de::from_str;

    use crate::{Device, DeviceList, RootDesc, Service, ServiceList};

    #[test]
    fn test_parse_root_desc() {
        let actual = from_str::<RootDesc>(include_str!("resources/test/devices-0.xml")).unwrap();
        let expected = RootDesc {
            devices: vec![Device {
                device_type: "urn:schemas-upnp-org:device:InternetGatewayDevice:1".into(),
                friendly_name: "ARRIS TG3492LG".into(),
                service_list: ServiceList {
                    services: vec![Service {
                        service_type: "urn:schemas-upnp-org:service:Layer3Forwarding:1".into(),
                        control_url: "/upnp/control/Layer3Forwarding".into(),
                        scpd_url: "/Layer3ForwardingSCPD.xml".into(),
                        event_sub_url: Some("/upnp/event/Layer3Forwarding".into()),
                    }],
                },
                device_list: DeviceList {
                    devices: vec![Device {
                        device_type: "urn:schemas-upnp-org:device:WANDevice:1".into(),
                        friendly_name: "WANDevice:1".into(),
                        service_list: ServiceList {
                            services: vec![Service {
                                service_type:
                                    "urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1".into(),
                                control_url: "/upnp/control/WANCommonInterfaceConfig0".into(),
                                scpd_url: "/WANCommonInterfaceConfigSCPD.xml".into(),
                                event_sub_url: Some("/upnp/event/WANCommonInterfaceConfig0".into()),
                            }],
                        },
                        device_list: DeviceList {
                            devices: vec![Device {
                                device_type: "urn:schemas-upnp-org:device:WANConnectionDevice:1"
                                    .into(),
                                friendly_name: "WANConnectionDevice:1".into(),
                                service_list: ServiceList {
                                    services: vec![Service {
                                        service_type:
                                            "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
                                        control_url: "/upnp/control/WANIPConnection0".into(),
                                        scpd_url: "/WANIPConnectionServiceSCPD.xml".into(),
                                        event_sub_url: Some("/upnp/event/WANIPConnection0".into()),
                                    }],
                                },
                                device_list: DeviceList { devices: vec![] },
                            }],
                        },
                    }],
                },
            }],
        };
        assert_eq!(actual, expected);
    }
}

/// The forwarder's loops against a stand-in router, on tokio's paused clock.
#[cfg(test)]
mod forwarder_tests {
    use std::{
        net::IpAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        sync::{mpsc::UnboundedSender, watch},
        time::Instant,
    };
    use url::Url;

    use crate::{
        FoundGateway, Gateways, PortForwardingState, SERVICE_TYPE_WAN_IP_CONNECTION,
        UpnpDiscoverResponse, UpnpPortForwarderOptions, WanService, run,
    };

    const PORT: u16 = 6881;
    const LOCATION: &str = "http://192.168.1.1:5000/rootDesc.xml";
    const CONTROL_URL: &str = "http://192.168.1.1:5000/ctl/IPConn";

    /// A router that answers searches while `present`, and accepts mappings
    /// while `accepts`. It records when each search happened.
    struct FakeRouter {
        present: AtomicBool,
        search_fails: AtomicBool,
        accepts: AtomicBool,
        /// AddPortMapping never answers.
        hangs: AtomicBool,
        /// How long a search takes, in seconds.
        search_secs: AtomicU64,
        /// We cannot tell our address on the gateway's network.
        no_local_ip: AtomicBool,
        start: Instant,
        searches: Mutex<Vec<Duration>>,
    }

    impl FakeRouter {
        fn new(present: bool, accepts: bool) -> Self {
            Self {
                present: AtomicBool::new(present),
                search_fails: AtomicBool::new(false),
                accepts: AtomicBool::new(accepts),
                hangs: AtomicBool::new(false),
                search_secs: AtomicU64::new(0),
                no_local_ip: AtomicBool::new(false),
                start: Instant::now(),
                searches: Mutex::new(Vec::new()),
            }
        }

        fn searched_at_secs(&self) -> Vec<u64> {
            let searches = self.searches.lock().unwrap();
            searches.iter().map(|d| d.as_secs()).collect()
        }
    }

    impl Gateways for FakeRouter {
        async fn search(
            &self,
            found: &UnboundedSender<UpnpDiscoverResponse>,
        ) -> anyhow::Result<usize> {
            self.searches.lock().unwrap().push(self.start.elapsed());
            let takes = Duration::from_secs(self.search_secs.load(Ordering::SeqCst));
            tokio::time::sleep(takes).await;
            if self.search_fails.load(Ordering::SeqCst) {
                anyhow::bail!("failed to send SSDP search request: Network is unreachable");
            }
            if !self.present.load(Ordering::SeqCst) {
                return Ok(0);
            }
            found.send(UpnpDiscoverResponse {
                received_from: "192.168.1.1:1900".parse().unwrap(),
                location: Url::parse(LOCATION).unwrap(),
            })?;
            Ok(1)
        }

        async fn describe(&self, response: UpnpDiscoverResponse) -> anyhow::Result<FoundGateway> {
            Ok(FoundGateway {
                name: "Test Router".into(),
                location: response.location,
                services: vec![WanService {
                    control_url: Url::parse(CONTROL_URL).unwrap(),
                    service_type: SERVICE_TYPE_WAN_IP_CONNECTION,
                }],
                local_ip: if self.no_local_ip.load(Ordering::SeqCst) {
                    Err(anyhow::anyhow!("couldn't find a local ip address"))
                } else {
                    Ok("192.168.1.2".parse().unwrap())
                },
            })
        }

        async fn add_port_mapping(
            &self,
            _service: &WanService,
            _local_ip: IpAddr,
            _port: u16,
            _lease_duration: Duration,
        ) -> anyhow::Result<()> {
            if self.hangs.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.accepts.load(Ordering::SeqCst) {
                Ok(())
            } else {
                anyhow::bail!("failed port forwarding: 500 Internal Server Error")
            }
        }
    }

    /// Runs the forwarder against `router` while `body` runs, then returns
    /// what `body` returned. The body is polled first, so it sees the state
    /// before the forwarder has done anything.
    async fn with_forwarder<F: Future>(
        router: &FakeRouter,
        opts: UpnpPortForwarderOptions,
        body: impl FnOnce(watch::Receiver<PortForwardingState>) -> F,
    ) -> F::Output {
        let state = watch::Sender::new(PortForwardingState::new(&[PORT]));
        let body = body(state.subscribe());
        let forwarder = run(&[PORT], &opts, router, &state);
        tokio::select! {
            biased;
            out = body => out,
            _ = forwarder => unreachable!("the forwarder runs forever"),
        }
    }

    async fn sleep_until_secs(start: Instant, secs: u64) {
        tokio::time::sleep_until(start + Duration::from_secs(secs)).await
    }

    #[tokio::test(start_paused = true)]
    async fn state_reports_search_gateway_and_held_mapping() {
        let router = &FakeRouter::new(true, true);
        with_forwarder(router, Default::default(), |rx| async move {
            {
                let s = rx.borrow();
                assert!(!s.searched);
                assert_eq!(s.gateway, None);
                assert!(!s.port(PORT).unwrap().held);
            }
            sleep_until_secs(router.start, 1).await;
            let s = rx.borrow().clone();
            assert!(s.searched);
            assert_eq!(s.discovery_error, None);
            let gateway = s.gateway.clone().unwrap();
            assert_eq!(gateway.name, "Test Router");
            assert_eq!(gateway.location, LOCATION);
            let p = s.port(PORT).unwrap();
            assert!(p.held);
            assert_eq!(p.held_via, vec![CONTROL_URL.to_owned()]);
            assert!(p.mapped_at.is_some());
            assert_eq!(p.error, None);
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn mapping_that_fails_to_renew_stops_being_held_when_its_lease_ends() {
        let router = &FakeRouter::new(true, true);
        // Lease 60 s, renewed every 30 s: mapped at 0, refused at 30 and 60.
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 1).await;
            assert!(rx.borrow().port(PORT).unwrap().held);
            router.accepts.store(false, Ordering::SeqCst);

            sleep_until_secs(router.start, 59).await;
            let s = rx.borrow().clone();
            let p = s.port(PORT).unwrap();
            assert!(p.held, "the lease taken at 0 runs until 60");
            assert!(p.error.is_some(), "the renewal at 30 was refused");

            sleep_until_secs(router.start, 61).await;
            let s = rx.borrow().clone();
            let p = s.port(PORT).unwrap();
            assert!(!p.held, "nothing renewed the lease that ended at 60");
            assert!(p.held_via.is_empty());
            assert!(p.error.is_some());
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn mapping_stops_being_held_when_its_lease_ends_during_a_renewal_that_hangs() {
        let router = &FakeRouter::new(true, true);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 1).await;
            assert!(rx.borrow().port(PORT).unwrap().held);
            // The renewal at 30 never comes back.
            router.hangs.store(true, Ordering::SeqCst);
            sleep_until_secs(router.start, 59).await;
            assert!(rx.borrow().port(PORT).unwrap().held);
            sleep_until_secs(router.start, 61).await;
            assert!(!rx.borrow().port(PORT).unwrap().held);
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn refused_mapping_is_not_held_and_says_why() {
        let router = &FakeRouter::new(true, false);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 1).await;
            let s = rx.borrow().clone();
            assert!(s.gateway.is_some());
            let p = s.port(PORT).unwrap();
            assert!(!p.held);
            assert_eq!(p.mapped_at, None);
            assert_eq!(
                p.error.as_deref(),
                Some("failed port forwarding: 500 Internal Server Error")
            );
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_search_clears_the_gateway_without_an_error() {
        let router = &FakeRouter::new(true, true);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 1).await;
            assert!(rx.borrow().gateway.is_some());

            // Nobody answers the search at 60.
            router.present.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 61).await;
            let s = rx.borrow().clone();
            assert_eq!(s.gateway, None);
            assert_eq!(s.discovery_error, None);
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn failed_search_is_a_discovery_error_and_no_answer_is_not() {
        let router = &FakeRouter::new(true, true);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 1).await;
            assert!(rx.borrow().gateway.is_some());

            // The next search, at 60, cannot even be sent.
            router.search_fails.store(true, Ordering::SeqCst);
            sleep_until_secs(router.start, 61).await;
            let s = rx.borrow().clone();
            assert!(s.searched);
            assert_eq!(s.gateway, None);
            assert_eq!(
                s.discovery_error.as_deref(),
                Some("failed to send SSDP search request: Network is unreachable")
            );

            // The one at 120 is sent, and nobody answers: no gateway, but no error.
            router.search_fails.store(false, Ordering::SeqCst);
            router.present.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 121).await;
            let s = rx.borrow().clone();
            assert_eq!(s.gateway, None);
            assert_eq!(s.discovery_error, None);
        })
        .await
    }

    type Captured = Arc<Mutex<Vec<(tracing::Level, String)>>>;

    std::thread_local! {
        static CAPTURING: std::cell::RefCell<Option<Captured>> = const { std::cell::RefCell::new(None) };
    }

    /// Collects this crate's log events on this thread, by level and message,
    /// while capturing. The tests run on a current-thread runtime, so that is
    /// all of the forwarder's events.
    ///
    /// The collector is the process-wide default, installed once, rather than
    /// a per-test scoped one: tracing caches whether a call site is wanted, and
    /// a call site first reached on another thread while no scoped collector
    /// wanted it can stay unwanted.
    #[derive(Default)]
    struct Logs(Captured);

    struct CaptureGuard;

    impl Drop for CaptureGuard {
        fn drop(&mut self) {
            CAPTURING.with(|c| c.borrow_mut().take());
        }
    }

    impl Logs {
        fn capture(&self) -> CaptureGuard {
            static INSTALL: std::sync::Once = std::sync::Once::new();
            INSTALL.call_once(|| {
                use tracing_subscriber::layer::SubscriberExt;
                tracing::subscriber::set_global_default(
                    tracing_subscriber::registry().with(ThreadCapture),
                )
                .expect("nothing else in these tests installs a global collector");
            });
            CAPTURING.with(|c| *c.borrow_mut() = Some(self.0.clone()));
            CaptureGuard
        }

        fn at(&self, level: tracing::Level) -> Vec<String> {
            let events = self.0.lock().unwrap();
            events
                .iter()
                .filter(|(l, _)| *l == level)
                .map(|(_, m)| m.clone())
                .collect()
        }
    }

    struct ThreadCapture;

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ThreadCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            if !event.metadata().target().starts_with("librqbit_upnp") {
                return;
            }
            CAPTURING.with(|c| {
                if let Some(captured) = c.borrow().as_ref() {
                    let mut message = Message(String::new());
                    event.record(&mut message);
                    captured
                        .lock()
                        .unwrap()
                        .push((*event.metadata().level(), message.0));
                }
            });
        }
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_backs_off_while_no_gateway_answers() {
        let router = &FakeRouter::new(false, true);
        with_forwarder(router, Default::default(), |_rx| async move {
            sleep_until_secs(router.start, 2200).await;
            // 60 s, doubling, up to 10 minutes.
            assert_eq!(
                router.searched_at_secs(),
                vec![0, 60, 180, 420, 900, 1500, 2100]
            );
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_backs_off_while_the_search_fails() {
        let router = &FakeRouter::new(true, true);
        router.search_fails.store(true, Ordering::SeqCst);
        with_forwarder(router, Default::default(), |_rx| async move {
            sleep_until_secs(router.start, 2200).await;
            assert_eq!(
                router.searched_at_secs(),
                vec![0, 60, 180, 420, 900, 1500, 2100]
            );
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_backoff_resets_when_a_gateway_answers() {
        let router = &FakeRouter::new(false, true);
        with_forwarder(router, Default::default(), |_rx| async move {
            sleep_until_secs(router.start, 500).await;
            router.present.store(true, Ordering::SeqCst);
            sleep_until_secs(router.start, 1030).await;
            // Found at 900; every minute from then on, as without back-off.
            assert_eq!(
                router.searched_at_secs(),
                vec![0, 60, 180, 420, 900, 960, 1020]
            );
            // Gone again: the back-off starts over from a minute.
            router.present.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 1270).await;
            assert_eq!(
                router.searched_at_secs(),
                vec![0, 60, 180, 420, 900, 960, 1020, 1080, 1140, 1260]
            );
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_backoff_resets_when_a_mapping_is_lost() {
        let router = &FakeRouter::new(true, true);
        let opts = UpnpPortForwarderOptions {
            lease_duration: Duration::from_secs(600),
            ..Default::default()
        };
        with_forwarder(router, opts, |rx| async move {
            sleep_until_secs(router.start, 1).await;
            assert!(rx.borrow().port(PORT).unwrap().held);
            // The gateway goes away: searches find nothing, renewals at 300
            // and 600 fail, and the lease taken at 0 ends at 600.
            router.present.store(false, Ordering::SeqCst);
            router.accepts.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 850).await;
            assert!(!rx.borrow().port(PORT).unwrap().held);
            // Backing off from 60: the search due at 960 comes at 660
            // instead, a minute after the loss, and backs off from there.
            assert_eq!(
                router.searched_at_secs(),
                vec![0, 60, 120, 240, 480, 660, 720, 840]
            );
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn no_gateway_warns_once_then_informs_when_one_answers() {
        let logs = Logs::default();
        let _guard = logs.capture();
        let router = &FakeRouter::new(false, true);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 500).await;
            assert!(rx.borrow().searched);
            assert_eq!(rx.borrow().discovery_error, None);
            router.present.store(true, Ordering::SeqCst);
            sleep_until_secs(router.start, 1000).await;
        })
        .await;
        // Searches at 0, 60, 180 and 420 found nothing; the one at 900 a gateway.
        let warns = logs.at(tracing::Level::WARN);
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].starts_with("no UPnP gateway answered"),
            "{warns:?}"
        );
        let quiet = logs.at(tracing::Level::DEBUG);
        assert_eq!(
            quiet
                .iter()
                .filter(|m| m.starts_with("no UPnP gateway answered"))
                .count(),
            3,
            "{quiet:?}"
        );
        assert_eq!(
            logs.at(tracing::Level::INFO),
            vec!["a UPnP gateway answered".to_owned()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failing_search_warns_once_then_informs_when_it_works() {
        let logs = Logs::default();
        let _guard = logs.capture();
        let router = &FakeRouter::new(true, true);
        router.search_fails.store(true, Ordering::SeqCst);
        with_forwarder(router, Default::default(), |_rx| async move {
            sleep_until_secs(router.start, 500).await;
            router.search_fails.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 1000).await;
        })
        .await;
        let warns = logs.at(tracing::Level::WARN);
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].starts_with("failed to run SSDP/UPNP discovery"),
            "{warns:?}"
        );
        assert_eq!(
            logs.at(tracing::Level::INFO),
            vec!["SSDP/UPNP discovery works again".to_owned()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refused_mapping_warns_once_then_informs_when_it_is_taken() {
        let logs = Logs::default();
        let _guard = logs.capture();
        let router = &FakeRouter::new(true, false);
        with_forwarder(router, Default::default(), |rx| async move {
            // Refused at 0, 30, 60 and 90; taken at 120.
            sleep_until_secs(router.start, 100).await;
            router.accepts.store(true, Ordering::SeqCst);
            sleep_until_secs(router.start, 121).await;
            let s = rx.borrow().clone();
            let p = s.port(PORT).unwrap();
            assert!(p.held);
            assert_eq!(p.error, None, "a mapping that is taken clears the error");
        })
        .await;
        let warns = logs.at(tracing::Level::WARN);
        assert_eq!(
            warns,
            vec!["failed to forward port 6881: failed port forwarding: 500 Internal Server Error"]
        );
        assert_eq!(
            logs.at(tracing::Level::INFO),
            vec!["port 6881 is forwarded again".to_owned()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn searches_keep_their_interval_however_long_they_take() {
        let router = &FakeRouter::new(true, true);
        router.search_secs.store(10, Ordering::SeqCst);
        with_forwarder(router, Default::default(), |_rx| async move {
            sleep_until_secs(router.start, 130).await;
            assert_eq!(router.searched_at_secs(), vec![0, 60, 120]);
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_local_address_warns_once() {
        let logs = Logs::default();
        let _guard = logs.capture();
        let router = &FakeRouter::new(true, true);
        router.no_local_ip.store(true, Ordering::SeqCst);
        with_forwarder(router, Default::default(), |rx| async move {
            sleep_until_secs(router.start, 200).await;
            assert!(!rx.borrow().port(PORT).unwrap().held);
            router.no_local_ip.store(false, Ordering::SeqCst);
            sleep_until_secs(router.start, 250).await;
            assert!(rx.borrow().port(PORT).unwrap().held);
        })
        .await;
        // Searches at 0, 60, 120 and 180 found the gateway; none could map.
        let warns = logs.at(tracing::Level::WARN);
        assert_eq!(warns.len(), 1, "{warns:?}");
        assert!(
            warns[0].starts_with("failed to determine local IP"),
            "{warns:?}"
        );
        let infos = logs.at(tracing::Level::INFO);
        assert_eq!(infos.len(), 1, "{infos:?}");
        assert!(infos[0].starts_with("determined local IP"), "{infos:?}");
    }

    #[tokio::test]
    async fn reply_nobody_can_read_is_not_a_warning() {
        let logs = Logs::default();
        let _guard = logs.capture();
        // Stands in for the multicast group: answers the first search with
        // something that is not a gateway's reply.
        let device = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let to = device.local_addr().unwrap();
        let answer = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (_, from) = device.recv_from(&mut buf).await.unwrap();
            device
                .send_to(b"NOTIFY * HTTP/1.1\r\nNTS: ssdp:alive\r\n\r\n", from)
                .await
                .unwrap();
        });
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let answered = crate::search(
            &tx,
            &crate::WAN_CONNECTION_SEARCH_TARGETS,
            Duration::from_secs(2),
            None,
            to,
        )
        .await
        .unwrap();
        answer.await.unwrap();
        assert_eq!(answered, 0);
        let parse_failures = |level| {
            logs.at(level)
                .into_iter()
                .filter(|m| m.starts_with("failed to parse SSDP response"))
                .count()
        };
        assert_eq!(parse_failures(tracing::Level::WARN), 0);
        assert_eq!(parse_failures(tracing::Level::DEBUG), 1);
    }
}

/// The DSL routers' WAN connection service, found and mapped through.
#[cfg(test)]
mod wan_ppp_tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use url::Url;

    use crate::{
        RootDesc, SERVICE_TYPE_WAN_IP_CONNECTION, SERVICE_TYPE_WAN_PPP_CONNECTION,
        SSDP_SEARCH_WAN_IPCONNECTION_ST, SSDP_SEARCH_WAN_PPPCONNECTION_ST, UpnpDiscoverResponse,
        UpnpEndpoint, WAN_CONNECTION_SEARCH_TARGETS, WanService, forward_port, search,
    };

    const PPP_GATEWAY: &str = r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
    <friendlyName>DSL Router</friendlyName>
    <deviceList>
      <device>
        <deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
        <deviceList>
          <device>
            <deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType>
            <serviceList>
              <service>
                <serviceType>urn:schemas-upnp-org:service:WANPPPConnection:1</serviceType>
                <controlURL>/upnp/control/WANPPPConn1</controlURL>
                <SCPDURL>/WANPPPConnSCPD.xml</SCPDURL>
              </service>
            </serviceList>
          </device>
        </deviceList>
      </device>
    </deviceList>
  </device>
</root>"#;

    #[test]
    fn searches_for_both_wan_connection_services() {
        assert!(WAN_CONNECTION_SEARCH_TARGETS.contains(&SSDP_SEARCH_WAN_IPCONNECTION_ST));
        assert!(WAN_CONNECTION_SEARCH_TARGETS.contains(&SSDP_SEARCH_WAN_PPPCONNECTION_ST));
    }

    #[tokio::test]
    async fn search_asks_for_each_target_and_counts_the_answers() {
        // Stands in for the multicast group: takes both searches, answers one.
        let gateway = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let to = gateway.local_addr().unwrap();
        let answer = tokio::spawn(async move {
            let mut asked = Vec::new();
            let mut buf = [0u8; 2048];
            for _ in 0..2 {
                let (n, from) = gateway.recv_from(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]).into_owned();
                asked.extend(
                    request
                        .lines()
                        .filter_map(|l| l.strip_prefix("ST: "))
                        .map(str::to_owned),
                );
                if asked.len() == 1 {
                    gateway
                        .send_to(
                            b"HTTP/1.1 200 OK\r\nLOCATION: http://192.168.1.1:49000/igddesc.xml\r\n\r\n",
                            from,
                        )
                        .await
                        .unwrap();
                }
            }
            asked
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let answered = search(
            &tx,
            &WAN_CONNECTION_SEARCH_TARGETS,
            // Long enough for the answer over loopback to arrive.
            std::time::Duration::from_secs(2),
            None,
            to,
        )
        .await
        .unwrap();
        assert_eq!(answered, 1);
        assert_eq!(
            rx.try_recv().unwrap().location.as_str(),
            "http://192.168.1.1:49000/igddesc.xml"
        );
        // Both searches went out before the search's own wait began, so they
        // have long arrived by now.
        answer.abort();
        let asked = answer.await;
        assert!(asked.is_ok(), "the gateway did not get a search per target");
        assert_eq!(asked.unwrap(), WAN_CONNECTION_SEARCH_TARGETS.to_vec());
    }

    #[test]
    fn manages_a_wan_ppp_connection_service() {
        let endpoint = UpnpEndpoint {
            discover_response: UpnpDiscoverResponse {
                received_from: "192.168.1.1:1900".parse().unwrap(),
                location: Url::parse("http://192.168.1.1:49000/igddesc.xml").unwrap(),
            },
            data: quick_xml::de::from_str::<RootDesc>(PPP_GATEWAY).unwrap(),
            nics: Vec::new(),
        };
        assert_eq!(
            endpoint.get_wan_services().collect::<Vec<_>>(),
            vec![WanService {
                control_url: Url::parse("http://192.168.1.1:49000/upnp/control/WANPPPConn1")
                    .unwrap(),
                service_type: SERVICE_TYPE_WAN_PPP_CONNECTION,
            }]
        );
    }

    /// Answers one HTTP request with 200 and returns the request as text.
    async fn answer_one_request(listener: tokio::net::TcpListener) -> String {
        let (mut conn, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = conn.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before the request was complete");
            request.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&request).into_owned();
            if let Some(head_end) = text.find("\r\n\r\n") {
                let length = text[..head_end]
                    .lines()
                    .find_map(|l| {
                        let (name, value) = l.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= head_end + 4 + length {
                    conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                    return text;
                }
            }
        }
    }

    #[tokio::test]
    async fn add_port_mapping_names_the_service_it_was_found_as() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(answer_one_request(listener));
        let service = WanService {
            control_url: Url::parse(&format!("http://{addr}/upnp/control/WANPPPConn1")).unwrap(),
            service_type: SERVICE_TYPE_WAN_PPP_CONNECTION,
        };
        forward_port(
            &service,
            "192.168.1.2".parse().unwrap(),
            6881,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
        let request = server.await.unwrap().to_lowercase();
        assert!(
            request.contains(&format!(
                "soapaction: \"{}#addportmapping\"",
                SERVICE_TYPE_WAN_PPP_CONNECTION.to_lowercase()
            )),
            "{request}"
        );
        assert!(
            request.contains(&format!(
                "<u:addportmapping xmlns:u=\"{}\">",
                SERVICE_TYPE_WAN_PPP_CONNECTION.to_lowercase()
            )),
            "{request}"
        );
        assert!(
            !request.contains(&SERVICE_TYPE_WAN_IP_CONNECTION.to_lowercase()),
            "{request}"
        );
    }
}
