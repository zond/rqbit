//! A live torrent has two locks its peer tasks take in a fixed order: a shard of the peer
//! table first, then the torrent's state lock (a dying peer holds its shard while it asks
//! whether the torrent is finished). `update_only_files()` used to take them the other way
//! round - re-queueing peers under the state lock - and one peer dying at the wrong moment
//! deadlocked the two, and with them every task that then touched either lock.
//!
//! What this test can and cannot catch: in debug builds the peer table asserts the order
//! on every access, so the inversion fails deterministically at the first call. In release
//! builds it is the race itself that has to happen: the selection is flipped in a tight
//! loop while thousands of peers die, which makes the window wide, but it is still a
//! window, and a deadlock is reported by a watchdog thread since a stuck runtime cannot
//! fire its own timeouts.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use librqbit_core::constants::CHUNK_SIZE;
use tempfile::TempDir;
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging, wait_until,
    },
};

const PIECE_LEN: u32 = CHUNK_SIZE;
const FILE_SIZE: usize = (PIECE_LEN * 8) as usize;
/// Peers to try. Each is a closed port on localhost, so each dies with an error the moment
/// it is tried - the path that holds a shard while asking the state lock. Many of them
/// also make the peer table big, so re-queueing under the state lock takes a while.
///
/// Fewer on Windows, where a connect to a closed localhost port is not refused at once:
/// the stack retries the SYN for about a second before it gives up, so 2000 of them could
/// not all die inside the wait below (the first CI run saw 924). The race still gets
/// hundreds of dying peers, and the debug builds' order assertion - which is what catches
/// an inversion deterministically - fires on the first call either way.
const PEERS: usize = if cfg!(windows) { 400 } else { 2000 };
/// Selections to flip between. Neither is ever finished (nothing is downloaded), so every
/// flip walks the whole peer table looking for peers to re-queue. It never finds one - a
/// refused connect leaves its peer dead, not not-needed - but it is the walk, not the
/// re-queueing, that used to happen under the state lock.
const SELECTIONS: [&[usize]; 2] = [&[0], &[0, 1]];
const FLIPPERS: usize = 2;
/// How long the deadlock gets to show up. Everything the test does is done in seconds.
const DEADLINE: Duration = Duration::from_secs(60);

#[test]
fn update_only_files_does_not_deadlock_with_dying_peers() {
    setup_test_logging();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // The wait happens off the runtime: a deadlocked runtime never fires a tokio timeout,
    // its workers being the ones stuck. And a stuck runtime is abandoned, not shut down,
    // as shutting it down would wait for those workers.
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = rt.handle().clone();
    std::thread::spawn(move || {
        let _ = tx.send(handle.block_on(run()));
    });
    match rx.recv_timeout(DEADLINE) {
        Ok(res) => {
            rt.shutdown_timeout(Duration::from_secs(10));
            res.unwrap();
        }
        Err(_) => {
            rt.shutdown_background();
            panic!(
                "the torrent deadlocked: update_only_files() did not return within {DEADLINE:?} while peers were dying"
            );
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let files = create_default_random_dir_with_torrents(2, FILE_SIZE, Some("rqbit_lock_order"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            piece_length: Some(PIECE_LEN),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let peers = closed_ports(PEERS)?;

    let dir = TempDir::with_prefix("rqbit_lock_order_client")?;
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                only_files: Some(SELECTIONS[0].to_vec()),
                initial_peers: Some(peers),
                disable_trackers: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;

    let stop = Arc::new(AtomicBool::new(false));
    let flippers: Vec<_> = (0..FLIPPERS)
        .map(|i| {
            let handle = handle.clone();
            let stop = stop.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
                let selections =
                    SELECTIONS.map(|files| files.iter().copied().collect::<HashSet<_>>());
                let mut flips = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    handle.update_only_files(&selections[(i + flips) % 2])?;
                    flips += 1;
                }
                Ok(flips)
            })
        })
        .collect();

    let dead = || {
        handle
            .stats()
            .live
            .map(|l| l.snapshot.peer_stats.dead as usize)
            .unwrap_or(0)
    };
    let all_dead = wait_until(
        || {
            let dead = dead();
            if dead < PEERS {
                bail!("{dead} of {PEERS} peers dead")
            }
            Ok(())
        },
        Duration::from_secs(30),
    )
    .await;
    // Stopped before the wait is checked: a flipper left running spins until the runtime is
    // torn down, and a failed wait would take that long to be reported.
    stop.store(true, Ordering::Relaxed);
    all_dead?;

    let mut flips = 0usize;
    for f in flippers {
        flips += f.await??;
    }
    info!(flips, dead = dead(), "done");
    if flips == 0 {
        bail!("the selection was never flipped")
    }
    Ok(())
}

/// Ports on localhost nothing listens on: the kernel handed them out as free and they were
/// released again. Connecting to one is refused right away. Bound in batches so as not to
/// run into the open files limit.
fn closed_ports(n: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let mut ports = HashSet::new();
    while ports.len() < n {
        let listeners = (0..256)
            .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
            .collect::<Result<Vec<_>, _>>()?;
        for l in &listeners {
            ports.insert(l.local_addr()?.port());
        }
    }
    Ok(ports
        .into_iter()
        .take(n)
        .map(|port| (Ipv4Addr::LOCALHOST, port).into())
        .collect())
}
