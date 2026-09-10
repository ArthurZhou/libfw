//! Integration tests for the native (non-WASM) Rust client against a real
//! `libfw-server` over TCP.
//!
//! These spin up the axum router on an ephemeral port and drive the client
//! end to end, which is what makes the protocol implementation verifiable
//! without a browser: uploads (session protocol + compression), resumable
//! downloads, resume sidecars, folder round-trips and adaptive tuning.
#![cfg(not(target_arch = "wasm32"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use libfw_client::ClientConfig;
use libfw_client::native::{NativeClient, NativeConfig, NativeEvent};
use libfw_client::TuneEvent;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{FsStorage, ServerState, router};

const TOKEN: &str = "dev-token";

#[derive(Clone)]
struct DevVerifier;

impl TokenVerifier for DevVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::Forbidden {
                path: "/".into(),
                action: libfw_core::Action::Read,
            });
        }
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".to_string()],
        })
    }
}

/// A live server on an ephemeral port, torn down when dropped.
struct TestServer {
    base_url: String,
    root: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
    /// Number of `/file/...` requests the server has seen.
    file_requests: Arc<std::sync::atomic::AtomicUsize>,
}

impl TestServer {
    fn file_requests(&self) -> usize {
        self.file_requests.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Counts `/file/...` requests (proves a download is chunked, not one big GET).
async fn count_file_requests(
    axum::extract::State(counter): axum::extract::State<Arc<std::sync::atomic::AtomicUsize>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if req.uri().path().starts_with("/file/") {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    next.run(req).await
}

/// Artificially slows every data request down, which keeps a transfer alive
/// long enough for progress *and* tuning updates to be observed mid-flight
/// (`/capabilities` stays fast so the ramp starts immediately).
async fn delay_requests(
    axum::extract::State(delay): axum::extract::State<std::time::Duration>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if !delay.is_zero() && !req.uri().path().starts_with("/capabilities") {
        tokio::time::sleep(delay).await;
    }
    next.run(req).await
}

async fn start_server() -> TestServer {
    start_slow_server(std::time::Duration::ZERO).await
}

async fn start_slow_server(delay: std::time::Duration) -> TestServer {
    // A tempdir per server: tests run in parallel threads of one process, so
    // a pid-based path would be shared (and deleted) across tests.
    let root = tempfile::tempdir().unwrap();
    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(root.path()))
            .verifier(DevVerifier)
            .validator(PathValidator::new())
            .build(),
    );
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app = router(state)
        .layer(axum::middleware::from_fn_with_state(
            counter.clone(),
            count_file_requests,
        ))
        .layer(axum::middleware::from_fn_with_state(delay, delay_requests));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    TestServer {
        base_url: format!("http://{addr}"),
        root,
        handle,
        file_requests: counter,
    }
}

/// A deterministic pseudo-random payload (compresses partially, so the zrip
/// path is actually exercised).
fn payload(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut state = 0x1234_5678u32;
    for _ in 0..len {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        // Mix compressible runs with incompressible bytes.
        out.push(if state.is_multiple_of(3) { (state >> 24) as u8 } else { 0x5A });
    }
    out
}

fn client(server: &TestServer, f: impl FnOnce(&mut ClientConfig)) -> NativeClient {
    let mut config = ClientConfig {
        // Small chunks keep the tests fast while still crossing many blocks.
        chunk_size: 256 * 1024,
        concurrency: 4,
        upload_window: 4,
        download_window: 4,
        max_retries: 3,
        timeout_ms: 30_000,
        ..ClientConfig::default()
    };
    f(&mut config);
    NativeClient::new(server.base_url.clone(), TOKEN, config)
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

/// The stored path of a virtual file on the server's filesystem root.
fn server_file(server: &TestServer, rel: &str) -> PathBuf {
    let mut path = server.root.path().to_path_buf();
    for segment in rel.split('/') {
        path.push(segment);
    }
    path
}

/// Sidecar path used by the client for `dest`.
fn sidecar_for(dest: &Path) -> PathBuf {
    PathBuf::from(format!("{}.libfw-resume.json", dest.display()))
}

#[tokio::test]
async fn upload_then_download_round_trips_with_compression() {
    let server = start_server().await;
    let client = client(&server, |_| {});
    let data = payload(3 * 1024 * 1024 + 12_345);

    let local = std::env::temp_dir().join(format!("libfw-native-src-{}.bin", std::process::id()));
    write_file(&local, &data);

    // --- upload
    let sent = client.upload_file(&local, "big.bin").await.unwrap();
    assert!(sent > 0, "a fresh upload must send bytes");
    let stored = std::fs::read(server_file(&server, "big.bin")).unwrap();
    assert_eq!(stored, data, "the server must hold the exact bytes");

    // --- download
    let dest = std::env::temp_dir().join(format!("libfw-native-dst-{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&dest);
    let moved = client.download_file("big.bin", &dest).await.unwrap();
    assert_eq!(moved, data.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    let _ = std::fs::remove_file(&local);
    let _ = std::fs::remove_file(&dest);
    let _ = std::fs::remove_file(sidecar_for(&dest));
}

#[tokio::test]
async fn uncompressed_round_trip_works_too() {
    let server = start_server().await;
    let client = client(&server, |c| c.compress = false);
    let data = payload(2 * 1024 * 1024 + 7);

    let local = std::env::temp_dir().join(format!("libfw-native-plain-{}.bin", std::process::id()));
    write_file(&local, &data);
    client.upload_file(&local, "dir/plain.bin").await.unwrap();
    assert_eq!(
        std::fs::read(server_file(&server, "dir/plain.bin")).unwrap(),
        data
    );
    let _ = std::fs::remove_file(&local);
}

#[tokio::test]
async fn download_resumes_from_a_partial_file() {
    let server = start_server().await;
    let client = client(&server, |_| {});
    let data = payload(2 * 1024 * 1024);

    let local = std::env::temp_dir().join(format!("libfw-native-res-{}.bin", std::process::id()));
    write_file(&local, &data);
    client.upload_file(&local, "resume.bin").await.unwrap();

    let dest = std::env::temp_dir().join(format!("libfw-native-resdst-{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&dest);

    // First download completes and leaves a resume sidecar with the ETag.
    let first = client.download_file("resume.bin", &dest).await.unwrap();
    assert_eq!(first, data.len() as u64);

    // Simulate an interrupted download: keep the first half on disk, and
    // rewind the sidecar to that offset.
    let half = data.len() as u64 / 2;
    let sidecar = sidecar_for(&dest);
    let json = std::fs::read_to_string(&sidecar).unwrap();
    let mut state: serde_json::Value = serde_json::from_str(&json).unwrap();
    state["offset"] = serde_json::json!(half);
    std::fs::write(&sidecar, state.to_string()).unwrap();
    let file = std::fs::OpenOptions::new().write(true).open(&dest).unwrap();
    file.set_len(half).unwrap();
    drop(file);

    // Second download must only fetch the remaining tail.
    let moved = client.download_file("resume.bin", &dest).await.unwrap();
    assert_eq!(moved, data.len() as u64 - half, "resume must skip the prefix");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // A third download is a no-op (nothing left to fetch).
    let again = client.download_file("resume.bin", &dest).await.unwrap();
    assert_eq!(again, 0);

    let _ = std::fs::remove_file(&local);
    let _ = std::fs::remove_file(&dest);
    let _ = std::fs::remove_file(&sidecar);
}

#[tokio::test]
async fn re_uploading_a_committed_file_is_idempotent() {
    let server = start_server().await;
    let client = client(&server, |_| {});
    let data = payload(1024 * 1024 + 5);
    let local = std::env::temp_dir().join(format!("libfw-native-dup-{}.bin", std::process::id()));
    write_file(&local, &data);

    let first = client.upload_file(&local, "dup.bin").await.unwrap();
    assert!(first > 0);
    // A committed session removes the server-side range sidecar, so a repeat
    // upload re-sends (there is no "already present" short-circuit for a
    // landed file) — but positional writes keep it idempotent.
    let second = client.upload_file(&local, "dup.bin").await.unwrap();
    assert!(second > 0);
    assert_eq!(std::fs::read(server_file(&server, "dup.bin")).unwrap(), data);
    let _ = std::fs::remove_file(&local);
}

#[tokio::test]
async fn folder_round_trip_preserves_structure() {
    let server = start_server().await;
    let client = client(&server, |c| c.chunk_size = 64 * 1024);
    let root = std::env::temp_dir().join(format!("libfw-native-tree-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    let a = payload(200 * 1024);
    let b = payload(70 * 1024);
    write_file(&root.join("one/a.bin"), &a);
    write_file(&root.join("one/two/b.bin"), &b);
    write_file(&root.join("empty-dir-placeholder.txt"), b"x");

    let uploaded = client.upload_folder(&root, "tree").await.unwrap();
    assert!(uploaded > 0);

    let dest = std::env::temp_dir().join(format!("libfw-native-treedst-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dest);
    client.download_folder("tree", &dest).await.unwrap();

    // The engine preserves the virtual paths it received, so everything lands
    // under `dest/tree/...` (same behaviour as the browser SDK).
    assert_eq!(std::fs::read(dest.join("tree/one/a.bin")).unwrap(), a);
    assert_eq!(std::fs::read(dest.join("tree/one/two/b.bin")).unwrap(), b);
    assert_eq!(
        std::fs::read(dest.join("tree/empty-dir-placeholder.txt")).unwrap(),
        b"x"
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&dest);
}

#[tokio::test]
async fn large_download_is_split_into_many_range_requests() {
    // Regression: a large download used to be fetched as ONE whole-file
    // request (a single 206 spanning the file) because the fresh adaptive
    // ramp starts at `download_window = 1` and the path decision was made
    // once. Now the chunked loop re-reads the window/chunk size per batch, so
    // even a minimal start still fetches the file in bounded chunks.
    let server = start_server().await;
    let client = client(&server, |c| {
        c.auto_tune = true;
        c.chunk_size = 256 * 1024;
    });
    let data = payload(8 * 1024 * 1024 + 1234);
    let local = std::env::temp_dir().join(format!("libfw-native-split-{}.bin", std::process::id()));
    write_file(&local, &data);
    client.upload_file(&local, "split.bin").await.unwrap();

    // Count only the download's requests: uploads are already done.
    let before = server.file_requests();
    let dest = std::env::temp_dir().join(format!("libfw-native-splitdst-{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&dest);
    let moved = client.download_file("split.bin", &dest).await.unwrap();
    let requests = server.file_requests() - before;

    assert_eq!(moved, data.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert!(
        requests >= 8,
        "an 8 MiB download must be fetched as many bounded ranges (saw {requests} \
         /file/ requests, including the HEAD probe)"
    );

    let _ = std::fs::remove_file(&local);
    let _ = std::fs::remove_file(&dest);
    let _ = std::fs::remove_file(sidecar_for(&dest));
}

#[tokio::test]
async fn adaptive_tuning_uses_the_advertised_limits() {
    let server = start_server().await;
    let client = client(&server, |c| c.auto_tune = true);
    let data = payload(1500 * 1024);
    let local = std::env::temp_dir().join(format!("libfw-native-tune-{}.bin", std::process::id()));
    write_file(&local, &data);

    client.upload_file(&local, "tuned.bin").await.unwrap();

    // The engine consulted `/capabilities` and is either still ramping or
    // already settled — but it must know which server it tuned against.
    let status = client.tune_status().expect("auto_tune enables the engine");
    assert!(!status.caps_hash.is_empty(), "caps hash must be recorded");
    assert!(status.params.chunk_size >= 1);
    assert!(status.params.concurrency >= 1);
    assert!(status.params.upload_window >= 1);

    let caps = client.capabilities().expect("capabilities were fetched");
    assert!(
        caps.limits.chunk_size.contains(status.params.chunk_size as i64),
        "tuned chunk size {} must stay inside the advertised range",
        status.params.chunk_size
    );

    let _ = std::fs::remove_file(&local);
}

#[tokio::test]
async fn unauthorized_requests_surface_the_status() {
    let server = start_server().await;
    // An empty token makes the verifier fail, which the server maps to 401.
    let client = NativeClient::new(server.base_url.clone(), "", ClientConfig::default());
    let err = client.download_file("nope.bin", "/tmp/libfw-should-not-exist.bin").await;
    match err {
        Err(libfw_client::LibfwError::Http { status, .. }) => {
            assert!(status == 401 || status == 403, "got {status}");
        }
        other => panic!("expected an auth error, got {other:?}"),
    }
}

#[tokio::test]
async fn missing_remote_file_is_404() {
    let server = start_server().await;
    let client = client(&server, |_| {});
    let err = client
        .download_file("does/not/exist.bin", "/tmp/libfw-nope.bin")
        .await;
    match err {
        Err(libfw_client::LibfwError::Http { status, .. }) => assert_eq!(status, 404),
        other => panic!("expected 404, got {other:?}"),
    }
}

#[tokio::test]
async fn events_report_progress_and_tuning() {
    let server = start_server().await;
    let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = {
        let events = events.clone();
        Arc::new(move |event: NativeEvent| {
            let label = match event {
                NativeEvent::FileStart { .. } => "fileStart",
                NativeEvent::Progress { .. } => "progress",
                NativeEvent::FileDone { .. } => "fileDone",
                NativeEvent::Tuning(_) => "tuning",
                NativeEvent::Log(_) => "log",
            };
            events.lock().unwrap().push(label.to_string());
        }) as libfw_client::native::EventSink
    };
    let client = NativeClient::with_config(
        NativeConfig::new(server.base_url.clone(), TOKEN)
            .with_client(ClientConfig {
                chunk_size: 64 * 1024,
                auto_tune: true,
                ..ClientConfig::default()
            })
            .with_events(sink),
    );
    let data = payload(400 * 1024);
    let local = std::env::temp_dir().join(format!("libfw-native-evt-{}.bin", std::process::id()));
    write_file(&local, &data);
    client.upload_file(&local, "evt.bin").await.unwrap();

    let seen = events.lock().unwrap().clone();
    assert!(seen.iter().any(|e| e == "fileStart"), "{seen:?}");
    assert!(seen.iter().any(|e| e == "progress"), "{seen:?}");
    assert!(seen.iter().any(|e| e == "fileDone"), "{seen:?}");
    let _ = std::fs::remove_file(&local);
}

#[tokio::test]
async fn tuning_state_is_process_local() {
    // The native client installs no `TuneStore`, so a settle is reused only by
    // the *same* client instance: a fresh client starts from the advertised
    // minimums again. (The browser engine caches its settle in `localStorage`.)
    let server = start_server().await;
    let data = payload(600 * 1024);
    let local = std::env::temp_dir().join(format!("libfw-native-local-{}.bin", std::process::id()));
    write_file(&local, &data);

    let shared = client(&server, |c| {
        c.auto_tune = true;
        c.chunk_size = 256 * 1024;
    });
    shared.upload_file(&local, "local1.bin").await.unwrap();
    let first = shared.tune_status().expect("auto_tune enables the engine");
    assert!(!first.caps_hash.is_empty());

    shared.upload_file(&local, "local2.bin").await.unwrap();
    let second = shared.tune_status().expect("engine stays enabled");
    assert_eq!(
        first.caps_hash, second.caps_hash,
        "the same client keeps one capabilities baseline across transfers"
    );

    // A second client shares nothing: no state is inherited, so it ramps from
    // the advertised minimums instead of reusing the first client's tune.
    let other = client(&server, |c| c.auto_tune = true);
    let fresh = other.tune_status().expect("auto_tune enables the engine");
    assert!(
        fresh.phase != libfw_client::TunePhase::Settled,
        "a new client must not inherit a settle"
    );

    // No tuning artefact is written next to the source file.
    assert!(!sidecar_for(&local).exists());
    let _ = std::fs::remove_file(&local);
}

/// Upload resume against the session protocol: an aborted upload must NOT be
/// re-sent from scratch — the retry probes the server and only sends the
/// blocks it is still missing.
#[tokio::test]
async fn upload_resumes_after_an_aborted_transfer() {
    let server = start_slow_server(std::time::Duration::from_millis(10)).await;
    let data = payload(2 * 1024 * 1024);
    let size = data.len() as u64;
    let local = std::env::temp_dir().join(format!("libfw-native-URES-{}.bin", std::process::id()));
    write_file(&local, &data);

    // Abort as soon as the server holds a quarter of the file.
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
    let sink = {
        let (tx, fired) = (tx.clone(), fired.clone());
        Arc::new(move |event: NativeEvent| {
            if let NativeEvent::Progress { done, total } = event {
                if total > 0 && done * 4 >= total {
                    fired.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _ = tx.lock().unwrap().take().map(|tx| tx.send(()));
                }
            }
        }) as libfw_client::native::EventSink
    };
    let client = NativeClient::with_config(
        NativeConfig::new(server.base_url.clone(), TOKEN)
            .with_client(ClientConfig {
                chunk_size: 64 * 1024,
                upload_window: 4,
                ..ClientConfig::default()
            })
            .with_events(sink),
    );

    tokio::select! {
        done = client.upload_file(&local, "ures.bin") => {
            // A very fast link can finish before the abort lands; the resume
            // assertion below still holds (nothing left to resend).
            done.unwrap();
        }
        _ = rx => {}
    }
    assert!(
        fired.load(std::sync::atomic::Ordering::SeqCst),
        "the transfer must have reported partial progress before the abort"
    );

    // The retry only sends what the server is still missing.
    let sent = client.upload_file(&local, "ures.bin").await.unwrap();
    assert!(
        sent < size,
        "the resumed upload must only send the missing blocks (sent {sent} of {size})"
    );
    assert_eq!(std::fs::read(server_file(&server, "ures.bin")).unwrap(), data);
    let _ = std::fs::remove_file(&local);
}

/// Progress and tuning must be *live*: several progress updates during the
/// transfer, and tuning events carrying real bandwidth / RTT samples plus the
/// parameters currently in force (all inside the advertised caps).
#[tokio::test]
async fn progress_and_tuning_are_reported_live() {
    // 300 ms per data request keeps the download running past the engine's
    // one-second measurement window, so the ramp reacts *during* the transfer.
    let server = start_slow_server(std::time::Duration::from_millis(300)).await;
    let data = payload(1536 * 1024);
    write_file(&server_file(&server, "live.bin"), &data);

    let progress = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let tuning = Arc::new(std::sync::Mutex::new(Vec::<TuneEvent>::new()));
    let sink = {
        let (progress, tuning) = (progress.clone(), tuning.clone());
        Arc::new(move |event: NativeEvent| match event {
            NativeEvent::Progress { done, .. } => progress.lock().unwrap().push(done),
            NativeEvent::Tuning(ev) => tuning.lock().unwrap().push(ev),
            _ => {}
        }) as libfw_client::native::EventSink
    };
    let client = NativeClient::with_config(
        NativeConfig::new(server.base_url.clone(), TOKEN)
            .with_client(ClientConfig {
                auto_tune: true,
                chunk_size: 64 * 1024,
                download_window: 2,
                ..ClientConfig::default()
            })
            .with_events(sink),
    );

    let dest = std::env::temp_dir().join(format!("libfw-native-LIVE-{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&dest);
    let moved = client.download_file("live.bin", &dest).await.unwrap();
    assert_eq!(moved, data.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // 1. Progress is monotonic and ends at the full size.
    let dones = progress.lock().unwrap().clone();
    assert!(
        dones.len() >= 2,
        "expected several progress updates, got {dones:?}"
    );
    assert!(dones.windows(2).all(|w| w[0] <= w[1]), "{dones:?}");
    assert_eq!(*dones.last().unwrap(), data.len() as u64);

    // 2. Tuning updates arrived *during* the transfer, with live samples.
    let tunes = tuning.lock().unwrap().clone();
    let last = tunes
        .last()
        .expect("a >1 s transfer must emit tuning events mid-flight");
    assert!(
        last.stats.mbps > 0.0,
        "live bandwidth must be reported: {:?}",
        last.stats
    );
    assert!(
        last.stats.rtt_ms > 0.0,
        "live RTT must be reported: {:?}",
        last.stats
    );
    // The same live configuration moved while the file was in flight: the
    // download window ramped above the advertised minimum (which is 1), and
    // the batch loop picked it up for the remaining chunks.
    assert!(
        tunes.iter().any(|e| e.params.download_window > 1),
        "the ramp must raise the live window during the transfer: {tunes:?}"
    );

    // 3. The same live state is visible through `tuneStatus()`, and the
    //    parameters in force respect the advertised caps.
    let status = client.tune_status().expect("auto_tune enables the engine");
    assert!(status.stats.mbps > 0.0, "{:?}", status.stats);
    let caps = client.capabilities().expect("capabilities were fetched");
    assert!(
        caps.limits.chunk_size.contains(status.params.chunk_size as i64),
        "tuned chunk size {} must stay inside the advertised range",
        status.params.chunk_size
    );
    assert!(caps.limits.download_window.contains(status.params.download_window as i64));

    let _ = std::fs::remove_file(&dest);
    let _ = std::fs::remove_file(sidecar_for(&dest));
}
