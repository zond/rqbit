# zond/rqbit: a fork of rqbit

This is zond's fork of [ikatson/rqbit](https://github.com/ikatson/rqbit), kept for [stream-server](https://github.com/zond/stream-server) and [xtremio](https://github.com/zond/xtremio). It is not meant to go upstream, and no pull requests are planned.

## Branches

- **`pinned`** is the branch consumers build. They depend on `librqbit` by git `rev`, not by branch, so a push here reaches them only when they bump the rev.
- `pinned` is rebased onto `upstream/main` from time to time, which rewrites its commit hashes. Some earlier tips are kept as `pinned-<short-sha>` tags.
- The other branches here (`main`, and branches that each carry a single change) are not what consumers build.

The fork publishes no binaries, crates, Docker images or desktop builds. The Releases, crates.io, docs.rs, Homebrew and Docker links in the upstream README below point at upstream's builds.

## What the fork adds

All of it is in `librqbit`, apart from the TLS change, which also covers `librqbit-upnp` and `upnp-serve`. The fork adds nothing to the `rqbit` CLI or the Web UI. The HTTP API gains two things: the stream route's `lookahead_bytes` query parameter and a `live_seeders` stats field.

**Piece reclaim: dropping pieces.** For keeping a bounded cache of a torrent larger than the disk.

- `AddTorrentOptions::piece_reclaim` (off by default) turns it on for one torrent. `add_torrent` refuses it unless the storage factory's `StorageFactory::ensure_can_release_pieces` says the storage can release a single piece. The default filesystem storage can't. `storage::examples::inmemory::InMemoryPieceStorageFactory` (feature `storage_examples`) is an example storage that can.
- `ManagedTorrent::drop_pieces(range)` forgets the pieces we have in that range, stops advertising them and stops wanting them. It also drops pieces we don't have. It returns a `DroppedPieces` claim: release the storage of `pieces()`, then drop the claim. Nothing downloads those pieces again while the claim is alive. It skips pieces a live stream's lookahead covers, pieces a peer is downloading or hash-checking, and pieces an earlier claim still holds. It works on a live or a paused torrent.
- A dropped piece is wanted again after `ManagedTorrent::reselect_pieces(range)`, after its file is re-selected with `update_only_files`, or when a stream's lookahead reaches it.
- The flag is persisted with the torrent. The set of dropped pieces is not. A restored reclaim torrent always starts paused, so the caller can drop what it doesn't want before it unpauses.
- `TorrentStorage::has_piece` lets a storage say at startup which pieces it still holds. A piece counts as ours only if the resume data (or the full check) and the storage both say so.

**Holding pieces back from announcements.** `ManagedTorrent::set_pieces_advertised(range, false)` leaves pieces out of the handshake bitfield and sends no Have for them. They are still downloaded, readable by streams, and served to a peer that asks for them. `set_pieces_advertised(range, true)` puts them back and sends connected peers a Have for each one we have. It works on a live or a paused torrent and needs no option. The set is not persisted. It survives a pause but not a re-check.

**Session upload switch.** `Session::set_upload_enabled(false)` chokes every peer of every torrent, including peers that connect later, and keeps them choked. Downloading carries on, and the bitfield and Haves still say what we have. `set_upload_enabled(true)` unchokes them again, and `Session::upload_enabled()` reads the switch. This is not the same as the `disable-upload` build feature, which hangs up on a peer that asks for data.

**Choke handback.** When a peer chokes us, the requests the choke discarded are forgotten and their pieces go back into the queue. Upstream left them reserved to that peer until a steal or a disconnect freed them.

**Runtime peer cap.** `ManagedTorrent::set_peer_limit(n)` changes a torrent's live-peer cap while it runs. Lowering it disconnects the surplus, least useful first: peers still connecting, then peers with nothing to exchange in either direction, then peers that moved the fewest bytes lately (sent and received count the same). Raising it re-dials the peers it parked that have an address we can dial, ahead of newly discovered addresses. `ManagedTorrentShared::peer_limit()` reads the cap. `TorrentStateLive::forget_disconnected_peers()` removes dead and parked entries from the peer table. `DEFAULT_PEER_LIMIT` is 128, upstream's default, and applies when neither the torrent nor the session sets a limit.

**Per-piece chunk progress and live-seeder stats.** `ManagedTorrent::piece_chunk_progress(piece)` returns a `PieceChunkProgress`: `downloaded_chunks` and `total_chunks` (16 KiB chunks), plus `verified`. The chunk count is downloaded, not verified, so it goes back to zero if the piece fails its hash check. The aggregate peer stats gain `live_seeders`, the number of connected peers that have the whole torrent. The HTTP API shows it in `GET /torrents/{id_or_infohash}/stats/v1`.

**Stream lookahead.** `FileStreamOptions { lookahead_bytes }`, passed to `ManagedTorrent::stream_with_options` or `Api::api_stream_with_options`, sets how far ahead of the reader pieces are prioritized. The default, `DEFAULT_STREAM_LOOKAHEAD_BYTES`, is upstream's 32 MiB. Over HTTP it is `GET /torrents/{id_or_infohash}/stream/{file_idx}?lookahead_bytes=N`. The server refuses 0 and anything over 1 GiB (1073741824) with 400.

**Storage.**

- Commit before have: `TorrentStorage::on_piece_completed` runs after the hash check and before the piece is marked have. If it returns an error, the torrent stops with a fatal error.
- Vectored writes: `Box<dyn TorrentStorage>` now forwards `pwrite_all_vectored`, and the storage middlewares (`slow`, `timing`, `write_through_cache`) forward it and `on_piece_completed`. Upstream they fell back to the trait defaults, so vectored writes never reached the storage and a wrapped storage never saw `on_piece_completed`. Both also forward the new `has_piece`.
- The filesystem storage can write past 2 GiB where `off_t` is 32 bits, as on 32-bit Android.
- The JSON session persistence store accepts any storage whose factory implements `StorageFactory::ensure_persistable`. Upstream it accepted only `FilesystemStorageFactory`. The default implementation refuses, and the filesystem storage accepts. (The Postgres store checks the storage neither here nor upstream.)

**Lock-order fix.** `update_only_files` no longer holds the torrent's state lock while it re-queues peers. Under peer churn, holding it could deadlock against a dying peer. Debug builds assert the lock order.

**TLS roots.** With `rust-tls` and without `default-tls`, every HTTP client that `librqbit` and `librqbit-upnp` build trusts only Mozilla's root certificates compiled into the binary (`webpki-root-certs`), not the platform store. `librqbit::http_client_builder()` returns a client builder with that policy, for embedders.

**CI on `pinned`.** `.github/workflows/test.yml` also runs on pushes to `pinned`, and one failing matrix entry no longer cancels the others (`fail-fast: false`).

## Behaviour if you don't opt in

This is meant to match upstream. `piece_reclaim` is off by default, and the code behind it is gated on that flag. Nothing is held back until you call `set_pieces_advertised`. Uploading stays on until you call `set_upload_enabled(false)`. The peer cap and the stream lookahead have upstream's defaults.

These changes apply to every user, opted in or not:

- **Chokes:** a choke hands back the requests it discarded (see above).
- **Not interested:** a peer's `NotInterested` now clears its interested flag. Upstream logged it and ignored it. So a finished torrent now disconnects a peer that has the whole torrent once that peer says it is no longer interested. Upstream kept such a peer, because the flag never went back to false.
- **Haves:** we send Haves to a peer that hasn't sent us a bitfield. Upstream read the empty bitfield as "already has it" and sent that peer none.
- **Piece picking:** a peer reserves a free piece before stealing one. The only steal ahead of the queue is the first piece of a stream's lookahead window, and only from a peer 10x slower. Upstream stole first.
- **Peer deaths and reconnects:** in-flight pieces are reserved to a connection, not just an address. They are handed back whatever state the peer's table entry is in. A dying connection no longer overwrites a newer connection's entry for the same address. Peers we have already talked to are re-dialled ahead of newly discovered addresses.
- **Writes:** a chunk that arrives in two parts of the peer's read buffer now reaches the filesystem storage's vectored write: one `pwritev` on Unix, and one write of the joined parts on other platforms. Upstream wrote the two parts with two separate writes, because `Box<dyn TorrentStorage>` did not forward the vectored call.
- **Streams:** a read that has to wait for a piece re-queues peers that were sent away and wakes connected peers that had nothing to request.
- **Bug fixes:**
  - the `update_only_files` lock order;
  - the chunk tracker now clears a piece's queue bit and counts the piece into its files at the moment it becomes have;
  - `wait_until_completed` no longer misses a completion that lands just as it starts waiting;
  - vectored writes past 2 GiB on 32-bit targets.
- **Storage implementers:** an error from `on_piece_completed` is now fatal to the torrent, and the call comes before the piece is marked have. Upstream called it afterwards and logged errors at debug level. `has_piece` (default `Ok(true)`) is asked at startup. The wrappers forward the methods listed above.
- **Persistence format:** JSON records gain a `piece_reclaim` field (a missing field reads as false). Postgres gets a `piece_reclaim BOOLEAN NOT NULL DEFAULT FALSE` column, added with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` when the store opens.
- **TLS under `rust-tls`:** a CA installed on the device, such as a corporate proxy or mitmproxy, no longer verifies rqbit's HTTPS. A root that Mozilla adds after the build is not trusted until you rebuild. Under `default-tls`, which the `rqbit` binary uses by default, nothing changes.

## Tests

The fork's tests are in `crates/librqbit/src/tests/`:

- `e2e_piece_reclaim.rs`
- `e2e_unadvertised_pieces.rs`
- `e2e_upload_switch.rs`
- `e2e_peer_limit.rs`
- `e2e_pause.rs`
- `lock_order.rs` and `session_persistence.rs`

There are also unit tests next to the code they cover (`chunk_tracker.rs`, `piece_tracker.rs`, the storage modules). `crates/librqbit/tests/tls_roots.rs` only builds with `rust-tls` and without `default-tls`, on Linux, so a default-feature `cargo test` (which is what CI runs) skips it.

---

*Everything below is upstream's README, unchanged apart from the notes marked "Fork note".*

---

[![crates.io](https://img.shields.io/crates/v/rqbit.svg)](https://crates.io/crates/rqbit)
[![crates.io](https://img.shields.io/crates/v/librqbit.svg)](https://crates.io/crates/librqbit)
[![docs.rs](https://img.shields.io/docsrs/librqbit.svg)](https://docs.rs/librqbit/latest/librqbit/)

# rqbit - bittorrent client in Rust

**rqbit** is a bittorrent client written in Rust. Has HTTP API and Web UI, and can be used as a library.

Also has a desktop app built with [Tauri](https://tauri.app/).

## Usage quick start

### Optional - start the server

Assuming you are downloading to ~/Downloads.

    rqbit server start ~/Downloads

### Download torrents

Assuming you are downloading to ~/Downloads. By default it'll download to current directory.

    rqbit download [-o ~/Downloads] 'magnet:?....' [https?://url/to/.torrent] [/path/to/local/file.torrent]

## Web UI

Access at http://localhost:3030/web/. See screenshot below (torrent names and speeds are simulated).

<img width="1000" src="https://github.com/user-attachments/assets/d916b3d9-ebbd-462a-889d-df3916cc2681" />

## Desktop app

The desktop app is a [thin wrapper](https://github.com/ikatson/rqbit/blob/main/desktop/src-tauri/src/main.rs) on top of the Web UI frontend.

Download it in [Releases](https://github.com/ikatson/rqbit/releases) for OSX and Windows. For Linux, build manually with

    cargo tauri build

It looks similar to the Web UI (screenshot above).

## Streaming support

rqbit can stream torrent files and smartly block the stream until the pieces are available. The pieces getting streamed are prioritized. All of this allows you to seek and live stream videos for example.

You can also stream to e.g. VLC or other players with HTTP URLs. Supports seeking too (through various range headers).
The streaming URLs look like http://IP:3030/torrents/<torrent_id>/stream/<file_id>

## Integrated UPnP Media Server

rqbit can advertise managed torrents to LAN, e.g. your TVs and stream torrents there (without transcoding). Seeking to arbitrary points in the videos is supported too.

Usage from CLI

```
rqbit --enable-upnp-server server start ...
```

## mDNS advertising

rqbit can advertise its HTTP API on your LAN via mDNS/DNS-SD, so you can open the Web UI at http://rqbit.local:3030/web/ from any device without knowing the server's IP.

Usage from CLI (requires a non-loopback listen address):

```
rqbit --enable-mdns --http-api-listen-addr 0.0.0.0:3030 server start ...
```

## IPv6

rqbit supports IPv6. By default it listens on all interfaces in dualstack mode. It can work even if there's no IPv6 enabled.

## Shell completions

Assuming bash, add this to your `~/.bashrc`. Modify for your shell of choice.

```
eval "$(rqbit completions bash)"
```

## Socks proxy support

```
rqbit --socks-url socks5://[username:password]@host:port ...
```

## Watching a directory for .torrents

```
rqbit server start --watch-folder [path] /download/path
```

## Systemd socket activation

rqbit can be started on-demand via [systemd socket activation](https://0pointer.de/blog/projects/socket-activation.html) by installing the [service and socket systemd units](systemd) into `$XDG_CONFIG_HOME/systemd/user/` (`~/.config/systemd/user`) and customizing them to your needs. If the associated [`rqbit.conf`](systemd/rqbit.conf) file is installed in `$XDG_CONFIG_HOME/rqbit/rqbit.conf` (`~/.config/rqbit/rqbit.conf`), it will be used to configure `rqbit` when started via the provided systemd unit.

## Performance

Anecdotally from a few reports, rqbit is faster than other clients they've tried, at least with their default settings.

Memory usage for the server is usually within a few tens of megabytes, which makes it great for e.g. RaspberryPI.

I've got a report that rqbit can saturate a 20Gbps link, although I don't have the hardware to confirm.

## Installation

> **Fork note:** everything in this section installs upstream's rqbit, not this fork. The fork publishes no builds; build it from source (see [Build](#build)).

There are pre-built binaries in [Releases](https://github.com/ikatson/rqbit/releases).

[![](https://repology.org/badge/vertical-allrepos/rqbit.svg)](https://repology.org/project/rqbit/versions)

### Homebrew

**rqbit** can be installed using Homebrew.
```sh
brew install rqbit
```

### Cargo

If you have the Rust toolchain installed then you can use the following.
```sh
cargo install rqbit
```

## Docker

> **Fork note:** these images are upstream's, not built from this fork.

Docker images are published at [ikatson/rqbit](https://hub.docker.com/r/ikatson/rqbit)

## Build

Just a regular Rust binary build process.

    cargo build --release

The "webui" feature requires npm installed.

## Some useful options

Run ```rqbit --help``` to see all available CLI options.

### -v <log-level>

Increase verbosity. Possible values: trace, debug, info, warn, error.

### --list

Will print the contents of the torrent file or the magnet link.

### --overwrite

If you want to resume downloading a file that already exists, you'll need to add this option.

### -r / --filename-re

Use a regex here to select files by their names.

## Features (not exhaustive)

### Supported BEPs

- [BEP-3: The BitTorrent Protocol Specification](https://www.bittorrent.org/beps/bep_0003.html)
- [BEP-5: DHT Protocol](https://www.bittorrent.org/beps/bep_0005.html)
- [BEP-7: IPv6 Tracker Extension](https://www.bittorrent.org/beps/bep_0007.html)
- [BEP-9: Extension for Peers to Send Metadata Files](https://www.bittorrent.org/beps/bep_0009.html)
- [BEP-10: Extension Protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [BEP-11: Peer Exchange (PEX)](https://www.bittorrent.org/beps/bep_0011.html)
- [BEP-12: Multitracker Metadata Extension](https://www.bittorrent.org/beps/bep_0012.html)
- [BEP-14: Local service discovery](https://www.bittorrent.org/beps/bep_0014.html)
- [BEP-15: UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)
- [BEP-20: Peer ID Conventions](https://www.bittorrent.org/beps/bep_0020.html)
- [BEP-23: Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html)
- [BEP-27: Private Torrents](https://www.bittorrent.org/beps/bep_0027.html)
- [BEP-29: uTorrent Transport Protocol](https://www.bittorrent.org/beps/bep_0029.html)
- [BEP-32: IPv6 extension for DHT](https://www.bittorrent.org/beps/bep_0032.html)
- [BEP-47: Padding files and extended file attributes](https://www.bittorrent.org/beps/bep_0047.html)
- [BEP-53: Magnet URI extension - Select specific file indices for download](https://www.bittorrent.org/beps/bep_0053.html)

### Some supported features

- Sequential downloading (the default and only option)
- Resume downloading file(s) if they already exist on disk
- Selective downloading using a regular expression for filename
- DHT support. Allows magnet links to work, and makes more peers available.
- HTTP API
- Pausing / unpausing / deleting (with files or not) APIs
- Stateful server
- Web UI
- Streaming, with seeking
- UPNP port forwarding to your router
- UPNP Media Server
- mDNS advertising
- Fastresume (no rehashing)
- Download / upload rate limiting
- Prometheus metrics at ```/metrics``` and ```/torrents/<id_or_infohash>/peer_stats/prometheus```

## HTTP API

By default it listens on http://127.0.0.1:3030.

```
curl -s 'http://127.0.0.1:3030/'

{
  "apis": {
    "GET /": "list all available APIs",
    "GET /dht/stats": "DHT stats",
    "GET /dht/table": "DHT routing table",
    "GET /metrics": "Prometheus metrics",
    "GET /stats": "Global session stats",
    "GET /stream_logs": "Continuously stream logs",
    "GET /torrents": "List torrents",
    "GET /torrents/playlist": "Playlist for supported players",
    "GET /torrents/{id_or_infohash}": "Torrent details",
    "GET /torrents/{id_or_infohash}/haves": "The bitfield of have pieces",
    "GET /torrents/{id_or_infohash}/metadata": "Download the corresponding torrent file",
    "GET /torrents/{id_or_infohash}/peer_stats": "Per peer stats",
    "GET /torrents/{id_or_infohash}/peer_stats/prometheus": "Per peer stats in prometheus format",
    "GET /torrents/{id_or_infohash}/playlist": "Playlist for supported players",
    "GET /torrents/{id_or_infohash}/stats/v1": "Torrent stats",
    "GET /torrents/{id_or_infohash}/stream/{file_idx}": "Stream a file. Accepts Range header to seek.",
    "GET /web/": "Web UI",
    "POST /rust_log": "Set RUST_LOG to this post launch (for debugging)",
    "POST /torrents": "Add a torrent here. magnet: or http:// or a local file.",
    "POST /torrents/create": "Create a torrent and start seeding. Body should be a local folder",
    "POST /torrents/resolve_magnet": "Resolve a magnet to torrent file bytes",
    "POST /torrents/{id_or_infohash}/add_peers": "Add peers (newline-delimited)",
    "POST /torrents/{id_or_infohash}/delete": "Forget about the torrent, remove the files",
    "POST /torrents/{id_or_infohash}/forget": "Forget about the torrent, keep the files",
    "POST /torrents/{id_or_infohash}/pause": "Pause torrent",
    "POST /torrents/{id_or_infohash}/start": "Resume torrent",
    "POST /torrents/{id_or_infohash}/update_only_files": "Change the selection of files to download. You need to POST json of the following form {\"only_files\": [0, 1, 2]}"
  },
  "server": "rqbit",
  "version": "9.0.0-beta.1"
}
```

### Basic auth

For HTTP API basic authentication set RQBIT_HTTP_BASIC_AUTH_USERPASS environment variable.

```
RQBIT_HTTP_BASIC_AUTH_USERPASS=username:password rqbit server start ...
```

### Add torrent through HTTP API

`curl -d 'magnet:?...' http://127.0.0.1:3030/torrents`

OR

`curl -d 'http://.../file.torrent' http://127.0.0.1:3030/torrents`

OR

`curl --data-binary @/tmp/xubuntu-23.04-minimal-amd64.iso.torrent http://127.0.0.1:3030/torrents`

Supported query parameters, all optional:

- overwrite=true|false
- only_files_regex - the regular expression string to match filenames
- output_folder - the folder to download to. If not specified, defaults to the one that rqbit server started with
- list_only=true|false - if you want to just list the files in the torrent instead of downloading

## Code organization

- crates/rqbit - main binary
- crates/librqbit - main library
- crates/librqbit-core - torrent utils
- crates/bencode - bencode serializing/deserializing
- crates/buffers - wrappers around binary buffers
- crates/clone_to_owned - a trait to make something owned
- crates/sha1w - wrappers around sha1 libraries
- crates/peer_binary_protocol - the protocol to talk to peers
- crates/dht - Distributed Hash Table implementation
- crates/upnp - upnp port forwarding
- crates/upnp_serve - upnp MediaServer
- desktop - desktop app built with [Tauri](https://tauri.app/)
- [librqbit-utp](https://github.com/ikatson/librqbit-utp/) - uTP protocol
- [librqbit-dualstack-sockets](https://github.com/ikatson/librqbit-dualstack-sockets) - cross-platform IPv6+IPv4 listeners with canonical IPs

## Motivation

This project began purely out of my enjoyment of writing code in Rust. I wasn’t satisfied with my regular BitTorrent client and wanted to see how much effort it would take to build one from scratch. Starting with the bencode protocol, then the peer protocol, it gradually evolved into what it is today.

## Donations and sponsorship

If you love rqbit, please consider donating through one of these methods. With enough support, I might be able to make this my full-time job one day — which would be amazing!

- [Github Sponsors](https://github.com/sponsors/ikatson)
- Crypto
  - ETH (Ethereum) 0x68c54b26b5372d5f091b6c08cc62883686c63527
  - XMR (Monero) 49LcgFreJuedrP8FgnUVB8GkAyoPX7A9PjWfKZA1hNYz5vPCEcYQ9HzKr3pccGR6Lc3V3hn52bukwZShLDhZsk57V41c2ea
  - XNO (Nano) nano_1ghid3z6x41x8cuoffb6bbrt4e14wsqdbyqwp5d8rk166meo3h77q7mkjusr
