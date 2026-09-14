#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Delivered certificate files: a key pair is swapped in whole, the previous pair is
//! kept until the apply outcome decides its fate, and an interrupted swap is repaired.

use guru_worker::certs;
use rpguru_sdk::orchestration_agent::CertificateFile;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("guru-worker-certs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pair(content: &str) -> Vec<CertificateFile> {
    vec![
        CertificateFile {
            path: "certs/acme/edge/full_chain.pem".to_string(),
            pem: format!("chain {content}"),
        },
        CertificateFile {
            path: "certs/acme/edge/key.pem".to_string(),
            pem: format!("key {content}"),
        },
        CertificateFile {
            path: "certs/ca.pem".to_string(),
            pem: format!("ca {content}"),
        },
    ]
}

fn read(state_dir: &Path, rel: &str) -> String {
    std::fs::read_to_string(state_dir.join(rel)).unwrap()
}

#[test]
fn a_failed_pod_keeps_the_previous_pair_and_an_applied_one_takes_the_new() {
    let state_dir = scratch("settle");
    let first = certs::write_files(&state_dir, &pair("v1")).unwrap();
    assert!(
        first.swapped().is_empty(),
        "a first write swaps nothing over"
    );
    first.settle(|_| false);
    assert_eq!(
        std::fs::metadata(state_dir.join("certs/acme/edge/key.pem"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    // A renewal replaces the pair; the previous one waits beside it.
    let edge = state_dir.join("certs/acme/edge");
    let second = certs::write_files(&state_dir, &pair("v2")).unwrap();
    assert_eq!(second.swapped(), std::slice::from_ref(&edge));
    assert_eq!(read(&state_dir, "certs/acme/edge/key.pem"), "key v2");
    assert_eq!(
        read(&state_dir, "certs/acme/edge/full_chain.pem"),
        "chain v2"
    );
    assert_eq!(read(&state_dir, "certs/ca.pem"), "ca v2");
    assert!(state_dir.join("certs/acme/edge.old").is_dir());

    // The pod reading it failed and still runs the old pair: the old pair comes back.
    second.settle(|dir| dir == edge);
    assert_eq!(read(&state_dir, "certs/acme/edge/key.pem"), "key v1");
    assert_eq!(
        read(&state_dir, "certs/acme/edge/full_chain.pem"),
        "chain v1"
    );
    assert!(!state_dir.join("certs/acme/edge.old").exists());
    assert!(!state_dir.join("certs/acme/edge.new").exists());

    // Next time the pod applies: the previous pair is dropped.
    let third = certs::write_files(&state_dir, &pair("v3")).unwrap();
    third.settle(|_| false);
    assert_eq!(read(&state_dir, "certs/acme/edge/key.pem"), "key v3");
    assert!(!state_dir.join("certs/acme/edge.old").exists());

    let _ = std::fs::remove_dir_all(&state_dir);
}

#[test]
fn an_interrupted_swap_is_repaired_at_startup() {
    let state_dir = scratch("recover");
    certs::write_files(&state_dir, &pair("v1"))
        .unwrap()
        .settle(|_| false);
    let edge = state_dir.join("certs/acme/edge");
    // A crash between the two renames: the old pair moved aside, the new one never in.
    std::fs::rename(&edge, state_dir.join("certs/acme/edge.old")).unwrap();
    std::fs::create_dir(state_dir.join("certs/acme/edge.new")).unwrap();
    std::fs::write(state_dir.join("certs/ca.pem.tmp"), "half").unwrap();

    certs::recover(&state_dir);
    assert_eq!(read(&state_dir, "certs/acme/edge/key.pem"), "key v1");
    assert_eq!(
        read(&state_dir, "certs/acme/edge/full_chain.pem"),
        "chain v1"
    );
    assert!(!state_dir.join("certs/acme/edge.old").exists());
    assert!(!state_dir.join("certs/acme/edge.new").exists());
    assert!(!state_dir.join("certs/ca.pem.tmp").exists());

    let _ = std::fs::remove_dir_all(&state_dir);
}

#[test]
fn a_path_that_escapes_the_state_directory_is_refused() {
    let state_dir = scratch("escape");
    for path in ["../outside.pem", "/etc/passwd", ""] {
        let err = certs::write_files(
            &state_dir,
            &[CertificateFile {
                path: path.to_string(),
                pem: "x".to_string(),
            }],
        )
        .expect_err(path);
        assert!(
            err.to_string().contains("not a plain relative path"),
            "{err}"
        );
    }
    let _ = std::fs::remove_dir_all(&state_dir);
}
