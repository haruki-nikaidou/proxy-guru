#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! `Supervisor::apply` is all-or-nothing: a config that cannot be fully bound must
//! leave the previously running listeners serving.

use guru_worker::supervisor::Supervisor;
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, ListenAs, LogConfig, Remote,
};
use std::net::SocketAddr;
use std::time::Duration;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

/// Two distinct free ports, in ascending order. Both probes are held until the pair is
/// chosen: one `free_port` call per port could hand back the same ephemeral port twice,
/// which would collapse the two listeners into a single key.
fn two_free_ports() -> (u16, u16) {
    let low = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let high = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let (a, b) = (
        low.local_addr().unwrap().port(),
        high.local_addr().unwrap().port(),
    );
    (a.min(b), a.max(b))
}

fn local(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
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
        forwardings,
    }
}

#[tokio::test]
async fn failed_apply_keeps_running_listeners() {
    let serving = local(free_port());
    let moved = local(free_port());
    // A foreign process owns this port; binding it must fail.
    let foreign = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken: SocketAddr = foreign.local_addr().unwrap();

    let mut sup = Supervisor::new();
    sup.apply(&config(vec![forwarding("serving", serving)]))
        .await
        .unwrap();
    tokio::net::TcpStream::connect(serving)
        .await
        .expect("first listener accepts");

    // The failing revision does not just add a listener: it also moves "serving" to
    // another port. A supervisor that mutates as it goes therefore tears the running
    // listener down (or brings the moved one up) before it discovers that "collides"
    // cannot bind — which is exactly what the assertions below catch.
    let err = sup
        .apply(&config(vec![
            forwarding("serving", moved),
            forwarding("collides", taken),
        ]))
        .await
        .expect_err("binding a taken port must fail the whole apply");
    assert_eq!(err.tag, "collides");

    // A cancelled accept loop needs a moment to drop its socket; wait it out so the
    // checks below cannot pass merely because the teardown has not landed yet.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // (i) The listener from the previous revision is still bound and still accepting.
    tokio::net::TcpStream::connect(serving)
        .await
        .expect("the previous revision's listener must still serve after a failed apply");
    // Nothing of the failed revision was half-committed: the socket bound for the
    // moved listener while preparing was released again.
    let probe = std::net::TcpListener::bind(moved)
        .expect("a failed apply must not leave the new listener bound");
    drop(probe);
    // ...and the failed apply released nothing of the foreign listener's port.
    assert!(std::net::TcpListener::bind(taken).is_err());

    // (ii) The supervisor's own state still describes the previous revision: applying
    // that same revision again is a pure retain. Had the supervisor forgotten the
    // listener while its socket stayed alive, this apply would fail to bind.
    sup.apply(&config(vec![forwarding("serving", serving)]))
        .await
        .expect("re-applying the running revision must be a no-op retain");
    tokio::net::TcpStream::connect(serving)
        .await
        .expect("the retained listener keeps serving");

    // (iii) A later good revision still converges: the move happens, and the listener
    // the supervisor claims to own really is the one it stops.
    sup.apply(&config(vec![forwarding("serving", moved)]))
        .await
        .unwrap();
    tokio::net::TcpStream::connect(moved)
        .await
        .expect("the moved listener accepts");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(serving).await.is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "the removed listener must stop accepting"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    sup.shutdown_all();
    drop(foreign);
}

/// Issue #11 (1): removing a listener cancelled its accept loop but let the socket be
/// closed by the spawned task afterwards, so a replacement on the same port could not
/// bind and the whole revision was rejected.
#[tokio::test]
async fn replacing_a_listener_on_the_same_port_succeeds_in_one_revision() {
    let port = free_port();
    let specific = local(port);
    let wildcard: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();

    let mut sup = Supervisor::new();
    sup.apply(&config(vec![forwarding("specific", specific)]))
        .await
        .unwrap();
    tokio::net::TcpStream::connect(specific)
        .await
        .expect("the first listener accepts");

    // Same port, different listener: the new socket can only bind once the old one is
    // gone, and a single revision has to be enough to get there.
    sup.apply(&config(vec![forwarding("wildcard", wildcard)]))
        .await
        .expect("replacing a listener on the same port must succeed in one revision");
    tokio::net::TcpStream::connect(specific)
        .await
        .expect("the replacement listener accepts on the same port");

    sup.shutdown_all();
}

/// Issue #11 (2): a hot-swap `send` into a listener whose accept loop has ended was
/// discarded while `apply` reported success, so the worker acked a config it was not
/// serving. The listener must be rebuilt instead.
#[tokio::test]
async fn apply_rebuilds_a_listener_whose_accept_loop_is_gone() {
    let serving = local(free_port());
    let mut sup = Supervisor::new();
    sup.apply(&config(vec![forwarding("serving", serving)]))
        .await
        .unwrap();
    tokio::net::TcpStream::connect(serving)
        .await
        .expect("the listener accepts");

    // Stop every accept loop behind the supervisor's back: the handles stay in the map
    // but the sockets they describe are gone.
    sup.shutdown_all();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(serving).await.is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "the cancelled listener never stopped accepting"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Re-applying the same revision must put a real listener back, not hand a config to
    // a receiver nobody holds and call that success.
    sup.apply(&config(vec![forwarding("serving", serving)]))
        .await
        .expect("re-applying must rebuild the dead listener");
    tokio::net::TcpStream::connect(serving)
        .await
        .expect("the rebuilt listener accepts");

    sup.shutdown_all();
}

/// A revision that vacates two addresses and then fails to bind one of the takeovers
/// must give both vacated listeners back — which it can only do if it first releases
/// the takeover socket it did manage to bind.
#[tokio::test]
async fn a_failed_takeover_gives_every_vacated_listener_back() {
    // Takeovers are attempted in address order, so the lower port is the one that binds
    // before the higher one fails: exactly the order in which a rollback has to undo a
    // takeover that already succeeded.
    let (low, high) = two_free_ports();
    let (taken_first, fails) = (local(low), local(high));
    // A foreign process owns `fails`'s port on another loopback address, so the wildcard
    // takeover of that port can never bind.
    let foreign = std::net::TcpListener::bind(("127.0.0.2", fails.port()))
        .expect("127.0.0.2 must be bindable for this test");
    let wildcard =
        |addr: SocketAddr| -> SocketAddr { format!("0.0.0.0:{}", addr.port()).parse().unwrap() };

    let mut sup = Supervisor::new();
    sup.apply(&config(vec![
        forwarding("takes-over", taken_first),
        forwarding("fails", fails),
    ]))
    .await
    .unwrap();

    let err = sup
        .apply(&config(vec![
            forwarding("takes-over", wildcard(taken_first)),
            forwarding("fails", wildcard(fails)),
        ]))
        .await
        .expect_err("the takeover of the foreign-held port must fail");
    assert_eq!(err.tag, "fails");

    tokio::net::TcpStream::connect(taken_first)
        .await
        .expect("the listener whose address was already taken over must be back");
    tokio::net::TcpStream::connect(fails)
        .await
        .expect("the listener whose takeover failed must be back");

    sup.shutdown_all();
    drop(foreign);
}
