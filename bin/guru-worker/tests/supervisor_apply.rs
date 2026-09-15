#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! `Supervisor::apply` commits a config per forwarding: every forwarding that can be
//! bound serves, and one that cannot keeps whatever listener its tag had before.

use guru_worker::supervisor::{ApplyOutcome, PodStatus, Supervisor};
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig, Remote,
};
use std::net::SocketAddr;
use std::time::Duration;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

/// `n` distinct free ports. Every probe is held until all are chosen: one `free_port`
/// call per port could hand back the same ephemeral port twice, which would collapse
/// two listeners into a single key.
fn free_ports(n: usize) -> Vec<u16> {
    let probes: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    probes
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

fn local(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn wildcard(port: u16) -> SocketAddr {
    format!("0.0.0.0:{port}").parse().unwrap()
}

fn forwarding(tag: &str, listen: SocketAddr) -> Forwarding {
    Forwarding {
        tag: tag.to_string(),
        listen,
        receive_proxy_protocol: None,
        listen_as: ListenAs::Raw,
        to: ForwardingTo::Exit {
            destination: Remote::parse("127.0.0.1:1").unwrap(),
            send_proxy_protocol: None,
        },
    }
}

fn config(forwardings: Vec<Forwarding>) -> Config {
    Config {
        ipv6_resolve: Ipv6Resolve::Tolerated,
        log: LogConfig::default(),
        relay_ca: None,
        keepalive: KeepAlive::default(),
        forwardings,
    }
}

fn error_of<'a>(outcome: &'a ApplyOutcome, tag: &str) -> Option<&'a str> {
    outcome
        .pods
        .iter()
        .find(|p| p.tag == tag)
        .unwrap_or_else(|| panic!("the outcome names {tag}"))
        .error
        .as_deref()
}

fn assert_all_applied(outcome: &ApplyOutcome) {
    assert!(
        outcome.failed().next().is_none(),
        "every forwarding applied: {:?}",
        outcome.pods
    );
}

async fn accepts(addr: SocketAddr, what: &str) {
    tokio::net::TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("{what}: {addr} must accept: {e}"));
}

async fn stops_accepting(addr: SocketAddr, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(addr).await.is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: {addr} must stop accepting"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn listen_of(cfg: &Config, tag: &str) -> SocketAddr {
    cfg.forwardings
        .iter()
        .find(|f| f.tag == tag)
        .unwrap_or_else(|| panic!("the running config names {tag}"))
        .listen
}

#[tokio::test]
async fn a_revision_commits_per_forwarding_and_a_failed_one_keeps_its_old_listener() {
    let ports = free_ports(3);
    let (good_old, bad_old, good_new) = (local(ports[0]), local(ports[1]), local(ports[2]));
    // A foreign process owns this port; binding it must fail.
    let foreign = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken: SocketAddr = foreign.local_addr().unwrap();

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![
            forwarding("good", good_old),
            forwarding("bad", bad_old),
        ]))
        .await,
    );

    // "good" moves to a free port and "bad" to a port it cannot bind.
    let outcome = sup
        .apply(&config(vec![
            forwarding("good", good_new),
            forwarding("bad", taken),
        ]))
        .await;
    assert_eq!(
        error_of(&outcome, "good"),
        None,
        "the bindable forwarding commits"
    );
    assert!(
        error_of(&outcome, "bad").is_some_and(|e| e.contains("in use")),
        "the failed forwarding reports its bind error: {:?}",
        outcome.pods
    );

    accepts(good_new, "the moved listener").await;
    stops_accepting(good_old, "the address the moved listener left").await;
    accepts(bad_old, "the failed tag's previous listener").await;
    assert!(
        std::net::TcpListener::bind(taken).is_err(),
        "nothing touched the foreign socket"
    );

    // The supervisor knows it is running the mix, and says so.
    let running = sup.running_config();
    assert_eq!(listen_of(&running, "good"), good_new);
    assert_eq!(listen_of(&running, "bad"), bad_old);
    let statuses = sup.pod_statuses();
    assert_eq!(
        statuses.iter().find(|p| p.tag == "good"),
        Some(&PodStatus {
            tag: "good".to_string(),
            error: None,
        })
    );
    assert!(
        statuses
            .iter()
            .any(|p| p.tag == "bad" && p.error.as_deref().is_some_and(|e| e.contains("in use"))),
        "pod_statuses carries the failed tag's error: {statuses:?}"
    );

    // Once the port frees up, re-applying the same revision converges.
    drop(foreign);
    assert_all_applied(
        &sup.apply(&config(vec![
            forwarding("good", good_new),
            forwarding("bad", taken),
        ]))
        .await,
    );
    accepts(taken, "the previously failed forwarding").await;
    stops_accepting(bad_old, "the failed tag's old address").await;
    assert!(sup.pod_statuses().iter().all(|p| p.error.is_none()));

    sup.shutdown_all();
}

/// Issue #11 (1): removing a listener cancelled its accept loop but let the socket be
/// closed by the spawned task afterwards, so a replacement on the same port could not
/// bind and the whole revision was rejected.
#[tokio::test]
async fn replacing_a_listener_on_the_same_port_succeeds_in_one_revision() {
    let port = free_port();
    let specific = local(port);

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("specific", specific)]))
            .await,
    );
    accepts(specific, "the first listener").await;

    // Same port, different listener: the new socket can only bind once the old one is
    // gone, and a single revision has to be enough to get there.
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("wildcard", wildcard(port))]))
            .await,
    );
    accepts(specific, "the replacement listener on the same port").await;
    assert_eq!(sup.running_config().forwardings.len(), 1);
    assert_eq!(listen_of(&sup.running_config(), "wildcard"), wildcard(port));

    sup.shutdown_all();
}

/// Issue #11 (2): a hot-swap `send` into a listener whose accept loop has ended was
/// discarded while `apply` reported success, so the worker acked a config it was not
/// serving. The listener must be rebuilt instead.
#[tokio::test]
async fn apply_rebuilds_a_listener_whose_accept_loop_is_gone() {
    let serving = local(free_port());
    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("serving", serving)]))
            .await,
    );
    accepts(serving, "the listener").await;

    // Stop every accept loop behind the supervisor's back: the handles stay in the map
    // but the sockets they describe are gone.
    sup.shutdown_all();
    stops_accepting(serving, "the cancelled listener").await;
    assert!(
        sup.pod_statuses()
            .iter()
            .any(|p| p.tag == "serving" && p.error.is_some()),
        "a stopped accept loop shows up as a pod error"
    );

    // Re-applying the same revision must put a real listener back, not hand a config to
    // a receiver nobody holds and call that success.
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("serving", serving)]))
            .await,
    );
    accepts(serving, "the rebuilt listener").await;

    sup.shutdown_all();
}

/// A forwarding that displaces its own listener to take a wider address on the same
/// port gets that listener back when the new bind fails, and the failure is its alone:
/// the other forwarding of the revision still commits.
#[tokio::test]
async fn a_failed_takeover_gives_the_displaced_listener_back() {
    let ports = free_ports(2);
    let (takes_over, fails) = (local(ports[0]), local(ports[1]));
    // A foreign process owns `fails`'s port on another loopback address, so the wildcard
    // takeover of that port can never bind.
    let foreign = std::net::TcpListener::bind(("127.0.0.2", fails.port()))
        .expect("127.0.0.2 must be bindable for this test");

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![
            forwarding("takes-over", takes_over),
            forwarding("fails", fails),
        ]))
        .await,
    );

    let outcome = sup
        .apply(&config(vec![
            forwarding("takes-over", wildcard(takes_over.port())),
            forwarding("fails", wildcard(fails.port())),
        ]))
        .await;
    assert_eq!(error_of(&outcome, "takes-over"), None);
    assert!(error_of(&outcome, "fails").is_some(), "{:?}", outcome.pods);

    accepts(takes_over, "the listener that took its port over").await;
    accepts(fails, "the listener whose takeover failed, restored").await;
    let running = sup.running_config();
    assert_eq!(
        listen_of(&running, "takes-over"),
        wildcard(takes_over.port())
    );
    assert_eq!(
        listen_of(&running, "fails"),
        fails,
        "the restored shape is the old one"
    );

    sup.shutdown_all();
    drop(foreign);
}

/// Old: A on key1, B on key2. New: A → key2, B → key3, and key3 cannot be bound. B must
/// keep key2, so A cannot have it: A fails naming B, and nothing is lost.
#[tokio::test]
async fn a_socket_still_held_by_a_tag_that_failed_to_move_is_not_taken() {
    let ports = free_ports(2);
    let (key1, key2) = (local(ports[0]), local(ports[1]));
    let foreign = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let key3: SocketAddr = foreign.local_addr().unwrap();

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("A", key1), forwarding("B", key2)]))
            .await,
    );

    let outcome = sup
        .apply(&config(vec![forwarding("A", key2), forwarding("B", key3)]))
        .await;
    assert!(
        error_of(&outcome, "B").is_some_and(|e| e.contains("in use")),
        "{:?}",
        outcome.pods
    );
    assert!(
        error_of(&outcome, "A").is_some_and(|e| e.contains("held by pod B")),
        "{:?}",
        outcome.pods
    );

    accepts(key1, "A's previous listener").await;
    accepts(key2, "B's previous listener").await;
    let running = sup.running_config();
    assert_eq!(listen_of(&running, "A"), key1);
    assert_eq!(listen_of(&running, "B"), key2);

    sup.shutdown_all();
    drop(foreign);
}

/// Old: A on key1, B on key2. New: A → key2, B → key3 (free). B moves first, then A may
/// claim the socket B left: both commit in one revision and key1 is released.
#[tokio::test]
async fn a_tag_may_claim_the_socket_of_a_tag_that_moved_in_the_same_revision() {
    let ports = free_ports(3);
    let (key1, key2, key3) = (local(ports[0]), local(ports[1]), local(ports[2]));

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("A", key1), forwarding("B", key2)]))
            .await,
    );
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("A", key2), forwarding("B", key3)]))
            .await,
    );

    accepts(key2, "A on the socket B left").await;
    accepts(key3, "B on its new socket").await;
    stops_accepting(key1, "the socket nobody runs any more").await;
    let running = sup.running_config();
    assert_eq!(listen_of(&running, "A"), key2);
    assert_eq!(listen_of(&running, "B"), key3);

    sup.shutdown_all();
}

/// Two tags swapping sockets need a socket neither holds: with none, both fail and
/// both keep what they had.
#[tokio::test]
async fn swapping_two_sockets_in_one_revision_fails_both_without_loss() {
    let ports = free_ports(2);
    let (key1, key2) = (local(ports[0]), local(ports[1]));

    let mut sup = Supervisor::new();
    assert_all_applied(
        &sup.apply(&config(vec![forwarding("A", key1), forwarding("B", key2)]))
            .await,
    );
    let outcome = sup
        .apply(&config(vec![forwarding("A", key2), forwarding("B", key1)]))
        .await;
    assert!(error_of(&outcome, "A").is_some_and(|e| e.contains("held by pod B")));
    assert!(error_of(&outcome, "B").is_some_and(|e| e.contains("held by pod A")));
    accepts(key1, "A's listener").await;
    accepts(key2, "B's listener").await;

    sup.shutdown_all();
}
