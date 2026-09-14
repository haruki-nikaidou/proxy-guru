//! Last-known-good config store.
//!
//! One file holds the revision and its TOML so the two can never disagree; it is
//! written after every apply with the config actually running — the revision's shape
//! for the pods that took it, the previous shape for the pods that did not — and
//! replayed at startup so the data plane comes up even while the master is unreachable.
//! Certificate paths in it are absolute, resolved against the state directory the
//! delivered files were written to.
//!
//! It is a restart optimisation, not a record of what the worker is serving: the
//! revision reported to the master comes from the last apply that actually
//! succeeded in this process, and a failed write here never fails an apply.

use crate::BoxError;
use std::path::Path;

const FILE_NAME: &str = "last-good.json";
const TMP_NAME: &str = "last-good.json.tmp";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LastKnownGood {
    pub revision: i64,
    pub toml: String,
}

/// Reads the stored config, or `None` when it is absent or unreadable.
pub fn load(state_dir: &Path) -> Option<LastKnownGood> {
    let path = state_dir.join(FILE_NAME);
    if !path.exists() {
        return None;
    }
    match std::fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str(&text) {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "invalid last-known-good state");
                None
            }
        },
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "unreadable last-known-good state");
            None
        }
    }
}

/// Atomically replaces the stored config.
pub fn store(state_dir: &Path, value: &LastKnownGood) -> Result<(), BoxError> {
    std::fs::create_dir_all(state_dir)?;
    let tmp = state_dir.join(TMP_NAME);
    let text = serde_json::to_string(value)?;
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, state_dir.join(FILE_NAME))?;
    Ok(())
}
