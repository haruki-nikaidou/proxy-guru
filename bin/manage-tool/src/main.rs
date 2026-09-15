//! # `manage-tool`
//!
//! Command-line administration tool for the application. Use it for tasks that
//! live outside the request path, such as:
//!
//! - running and validating database migrations,
//! - seeding default configuration into the database,
//! - creating and managing administrator accounts,
//! - one-off maintenance and data-fix commands.
//!
//! See `bin/manage-tool/README.md` for the full description.

use auth::config::AuthConfig;
use auth::entities::surreal::account::{AccountRole, CreateAccount, FindAccountByEmail};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::entities::surreal::app_config::{ConfigJson, FindRawConfig};
use base::services::config::{
    ConfigError, ConfigStore, LoadConfig, SeedConfig, StoreConfig, decode, defaults,
};
use clap::{Parser, Subcommand};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::surreal::agent_release::PublishAgentRelease;
use orchestration::entities::surreal::ca::{FindInternalCa, ListRelayCertificatesByPods};
use orchestration::entities::surreal::certificate::ListCertificatesBySnis;
use orchestration::services::OrchestrationError;
use orchestration::services::ca::{CaService, InitInternalCa};
use orchestration::services::derive::{
    DerivationCertificates, derive_server_config, relay_tls_pods, tls_snis,
};
use orchestration::utils::secret::SecretKey;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use surrealdb::opt::auth::Root;
use surrealdb::types::ToSql;
use wakuwaku::surreal::SurrealProcessor;

/// Administration CLI.
#[derive(Debug, Parser)]
#[command(name = "manage-tool", about = "Administration tasks for the platform")]
struct Cli {
    /// SurrealDB address (e.g. `ws://127.0.0.1:8000`).
    #[arg(long, env = "SURREALDB_HOST", default_value = "ws://127.0.0.1:8000")]
    address: String,
    /// Root username.
    #[arg(long, env = "SURREALDB_USER", default_value = "root")]
    username: String,
    /// Root password.
    #[arg(long, env = "SURREALDB_PASSWORD", default_value = "root")]
    password: String,
    /// Namespace to operate in. Required by every subcommand that touches the
    /// database.
    #[arg(long, env = "SURREALDB_NAMESPACE")]
    namespace: Option<String>,
    /// Database to operate in. Required by every subcommand that touches the
    /// database.
    #[arg(long, env = "SURREALDB_NAME")]
    database: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bootstrap the first administrator account.
    CreateAdmin {
        /// Administrator email address.
        #[arg(long)]
        email: String,
        /// Administrator password.
        #[arg(long)]
        password: String,
    },
    /// Print a fresh `GURU_MASTER_KEY` (32 random bytes, base64). Needs no
    /// database.
    GenerateMasterKey,
    /// The database-backed configuration store.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Orchestration maintenance.
    Orchestration {
        #[command(subcommand)]
        command: OrchestrationCommand,
    },
    /// The `guru-worker` agent distribution: what the dashboard's install
    /// command downloads and what a running worker updates to.
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Write the defaults for every registered key that has none. Idempotent:
    /// a key an operator has edited is left untouched. Run it after
    /// `surrealkit sync`.
    Seed,
    /// Print every registered key with its stored document, or the defaults
    /// when it has none.
    List,
    /// Print one key's stored document, or the defaults when it has none. Not
    /// decoded, so a row that fails a master's startup read is still readable.
    Get {
        /// Config key, e.g. `orchestration`.
        key: String,
    },
    /// Replace one key's stored JSON. The payload is validated against the
    /// config's type before it is written; masters pick it up on restart.
    Set {
        /// Config key, e.g. `orchestration`.
        key: String,
        /// The whole config as a JSON object.
        json: String,
    },
}

#[derive(Debug, Subcommand)]
enum OrchestrationCommand {
    /// Print the derived guru-worker TOML for one server.
    ExportConfig {
        /// `orchestration_server` record key.
        #[arg(long)]
        server: String,
    },
    /// Create the internal CA that signs relay TLS/QUIC certificates and print
    /// its certificate. Refuses to replace an existing CA. Needs
    /// `GURU_MASTER_KEY`.
    InitCa,
}

#[derive(Debug, Subcommand)]
enum AgentCommand {
    /// Publish a built `guru-worker`: copy it under its version into the
    /// directory nginx serves as `/agent/`, refresh the installer, systemd unit
    /// and start guard next to it, and record its version, SHA-256 and
    /// architecture so the dashboard offers it. The binary is run once
    /// (`--version`), so publish on a host that can execute it.
    Publish {
        /// The built binary, e.g. `target/release/guru-worker`.
        #[arg(long)]
        binary: PathBuf,
        /// The directory nginx serves, e.g. `/srv/guru/agent`.
        #[arg(long)]
        dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Command::GenerateMasterKey = cli.command {
        println!("{}", SecretKey::generate_base64());
        return Ok(());
    }

    let (Some(namespace), Some(database)) = (cli.namespace.as_deref(), cli.database.as_deref())
    else {
        return Err(
            "--namespace (SURREALDB_NAMESPACE) and --database (SURREALDB_NAME) are \
                    required for this subcommand"
                .into(),
        );
    };
    let db = surrealdb::engine::any::connect(&cli.address).await?;
    db.signin(Root {
        username: cli.username,
        password: cli.password,
    })
    .await?;
    db.use_ns(namespace).use_db(database).await?;

    match cli.command {
        Command::CreateAdmin { email, password } => {
            create_admin(SurrealProcessor::new(db), email, password).await
        }
        Command::GenerateMasterKey => Ok(()),
        Command::Config { command } => config(SurrealProcessor::new(db), command).await,
        Command::Orchestration {
            command: OrchestrationCommand::ExportConfig { server },
        } => export_config(SurrealProcessor::new(db), server).await,
        Command::Orchestration {
            command: OrchestrationCommand::InitCa,
        } => init_ca(SurrealProcessor::new(db)).await,
        Command::Agent {
            command: AgentCommand::Publish { binary, dir },
        } => agent_publish(SurrealProcessor::new(db), binary, dir).await,
    }
}

/// Every config key this installation owns.
///
/// One variant per key, and every operation below dispatches on it, so adding a
/// key is a compile error until it is wired everywhere — the CLI cannot drift
/// from the set of configs the binaries load.
#[derive(Debug, Clone, Copy)]
enum ConfigKey {
    Auth,
    Orchestration,
}

impl ConfigKey {
    const ALL: [Self; 2] = [Self::Auth, Self::Orchestration];

    fn name(self) -> &'static str {
        match self {
            Self::Auth => AuthConfig::KEY,
            Self::Orchestration => OrchestrationConfig::KEY,
        }
    }

    fn parse(key: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.name() == key)
            .ok_or_else(|| {
                let known = Self::ALL
                    .into_iter()
                    .map(Self::name)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("unknown config key `{key}`; registered keys: {known}").into()
            })
    }

    /// Insert the defaults when the key is absent; `true` when it was created.
    async fn seed(self, store: &ConfigStore) -> Result<bool, ConfigError> {
        match self {
            Self::Auth => store.process(SeedConfig::<AuthConfig>::new()).await,
            Self::Orchestration => {
                store
                    .process(SeedConfig::<OrchestrationConfig>::new())
                    .await
            }
        }
    }

    /// Validate the payload against the config's type, then store it.
    async fn set(self, store: &ConfigStore, content: serde_json::Value) -> Result<(), ConfigError> {
        match self {
            Self::Auth => {
                store
                    .process(StoreConfig(decode::<AuthConfig>(content)?))
                    .await
            }
            Self::Orchestration => {
                store
                    .process(StoreConfig(decode::<OrchestrationConfig>(content)?))
                    .await
            }
        }
    }

    /// The payload `seed` would write. Printed for a key that has no row yet,
    /// so `get` still answers with what a master would run.
    fn defaults(self) -> Result<serde_json::Value, ConfigError> {
        match self {
            Self::Auth => defaults::<AuthConfig>(),
            Self::Orchestration => defaults::<OrchestrationConfig>(),
        }
    }
}

/// Loads the orchestration config the masters run with. Keeping the CLI on the
/// same values is what makes an exported config match what a master derives.
async fn orchestration_config(
    db: &SurrealProcessor,
) -> Result<OrchestrationConfig, Box<dyn std::error::Error>> {
    let store = ConfigStore { db: db.clone() };
    Ok(store.process(LoadConfig::new()).await?)
}

/// The configuration store: seed defaults, inspect and replace values.
///
/// `list` and `get` print the row as stored, without decoding it: a document
/// that no longer matches its type is exactly what an operator needs to see,
/// and `set` is the way back out.
async fn config(
    db: SurrealProcessor,
    command: ConfigCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = ConfigStore { db };
    match command {
        ConfigCommand::Seed => {
            for key in ConfigKey::ALL {
                if key.seed(&store).await.map_err(|e| e.to_string())? {
                    println!("seeded {}", key.name());
                } else {
                    println!("{} already set, left untouched", key.name());
                }
            }
        }
        ConfigCommand::List => {
            for key in ConfigKey::ALL {
                let (state, value) = document(key, &store).await?;
                println!("# {} ({state})", key.name());
                println!("{}\n", serde_json::to_string_pretty(&value)?);
            }
        }
        ConfigCommand::Get { key } => {
            let key = ConfigKey::parse(&key)?;
            let (_, value) = document(key, &store).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        ConfigCommand::Set { key, json } => {
            let key = ConfigKey::parse(&key)?;
            let content: serde_json::Value = serde_json::from_str(&json)
                .map_err(|e| format!("the payload is not valid JSON: {e}"))?;
            // A payload that is not this config is an operator typo, not a bug:
            // report it plainly and leave the row alone.
            if let Err(error) = key.set(&store, content).await {
                eprintln!("{error}");
                std::process::exit(1);
            }
            eprintln!(
                "{} updated; restart guru-master for it to take effect",
                key.name()
            );
        }
    }
    Ok(())
}

/// One key's document: the stored row, or the defaults when it has none.
async fn document(
    key: ConfigKey,
    store: &ConfigStore,
) -> Result<(&'static str, serde_json::Value), Box<dyn std::error::Error>> {
    // Raw read straight through the entity layer: an operator inspecting a row
    // that no longer matches its type must see it undecoded.
    match store.db.process(FindRawConfig { key: key.name() }).await? {
        Some(value) => Ok(("stored", value)),
        None => Ok(("unset, showing defaults", key.defaults()?)),
    }
}

/// Generates the internal CA. Every canvas tree holding a TLS/QUIC relay is
/// marked for re-derivation so its pods get their first leaf certificates.
async fn init_ca(db: SurrealProcessor) -> Result<(), Box<dyn std::error::Error>> {
    let config = orchestration_config(&db).await?;
    let ca = CaService {
        db,
        secrets: SecretKey::from_env()?,
        config,
    };
    let init = match ca.process(InitInternalCa).await {
        Ok(init) => init,
        Err(OrchestrationError::Conflict(message)) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
        Err(e) => return Err(e.into()),
    };
    print!("{}", init.certificate_pem);
    for canvas in &init.touched_canvases {
        eprintln!(
            "canvas {} marked for re-derivation",
            orchestration::utils::ids::record_key(&canvas.0)
        );
    }
    Ok(())
}

/// Derive one server's *ideal* worker config from the live canvas and print it.
/// No identity is involved: the CLI already authenticates against the database
/// itself. This is the config the canvas asks for, before convergence trims it to
/// what the rest of the fabric can support today.
async fn export_config(
    db: SurrealProcessor,
    server: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_id = orchestration::utils::ids::server_id(&server);
    let Some(row) = db
        .process(orchestration::entities::surreal::server::FindServerById {
            id: server_id.clone(),
        })
        .await?
    else {
        eprintln!("No server with key {server}");
        std::process::exit(1);
    };
    let topology = db
        .process(
            orchestration::entities::surreal::topology::LoadCanvasTopology { canvas: row.canvas },
        )
        .await?;
    let certificates = DerivationCertificates {
        acme: db
            .process(ListCertificatesBySnis {
                snis: tls_snis(&topology),
            })
            .await?,
        relay: db
            .process(ListRelayCertificatesByPods {
                pods: relay_tls_pods(&topology),
            })
            .await?,
        ca_present: db.process(FindInternalCa).await?.is_some(),
        assume_issued: false,
    };
    let config = orchestration_config(&db).await?;
    let derived = derive_server_config(&topology, &server_id, &certificates, &config)?;
    print!("{}", derived.config.to_toml_string()?);
    Ok(())
}

/// Create the first administrator account directly via the entity layer (no
/// acting admin exists yet, so RBAC is deliberately bypassed for bootstrap).
async fn create_admin(
    db: SurrealProcessor,
    email: String,
    password: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let email = email.trim().to_lowercase();

    if db
        .process(FindAccountByEmail { email: &email })
        .await?
        .is_some()
    {
        eprintln!("An account with email {email} already exists");
        std::process::exit(1);
    }

    let password_hash = Argon2PasswordAlgorithm::default().hash_password(&password)?;
    let account = db
        .process(CreateAccount {
            email,
            password_hash,
            role: AccountRole::Admin,
        })
        .await?;

    println!("Created admin account {}", account.id.0.to_sql());
    Ok(())
}

/// What `agent publish` ships next to the binary. Embedded, so the tool needs
/// no checkout at runtime and the three always come from the same source
/// revision as the binary they accompany.
const INSTALL_SH: &str = include_str!("../../guru-worker/deploy/install.sh");
const UNIT_FILE: &str = include_str!("../../guru-worker/deploy/guru-worker@.service");
const GUARD_SH: &str = include_str!("../../guru-worker/deploy/guru-worker-guard");

/// Publishes a built `guru-worker` for the install command and self-update.
///
/// The version is read by running the exact bytes being published, so the
/// recorded version can never disagree with the file; the architecture comes
/// from the ELF header. Files are written through a sibling temp file and
/// renamed, so a download racing the publish gets the old file or the new one.
async fn agent_publish(
    db: SurrealProcessor,
    binary: PathBuf,
    dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = std::fs::read(&binary).map_err(|e| format!("reading {}: {e}", binary.display()))?;
    let arch = elf_arch(&bytes)
        .ok_or("the binary is not an ELF executable for a supported architecture")?;
    let version = worker_version(&binary)?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));

    let version_dir = dir.join(&version);
    std::fs::create_dir_all(&version_dir)
        .map_err(|e| format!("creating {}: {e}", version_dir.display()))?;
    publish_file(&version_dir.join("guru-worker"), &bytes, 0o755)?;
    publish_file(&dir.join("install.sh"), INSTALL_SH.as_bytes(), 0o644)?;
    publish_file(
        &dir.join("guru-worker@.service"),
        UNIT_FILE.as_bytes(),
        0o644,
    )?;
    publish_file(&dir.join("guru-worker-guard"), GUARD_SH.as_bytes(), 0o644)?;

    db.process(PublishAgentRelease {
        version: version.clone(),
        sha256: sha256.clone(),
        arch: arch.to_string(),
        now: chrono::Utc::now(),
    })
    .await?;
    println!(
        "published guru-worker {version} ({arch}) to {}",
        version_dir.display()
    );
    println!("  sha256 {sha256}");
    println!("  the dashboard now offers this version to servers running another one");
    Ok(())
}

/// `guru-worker --version` prints `guru-worker <version>`.
fn worker_version(binary: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = std::process::Command::new(binary)
        .arg("--version")
        .output()
        .map_err(|e| format!("running {} --version: {e}", binary.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} --version failed: {}",
            binary.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .split_whitespace()
        .last()
        .map(str::to_owned)
        .ok_or_else(|| format!("{} --version printed nothing", binary.display()).into())
}

/// The architecture an ELF binary was built for, from its `e_machine` field.
fn elf_arch(bytes: &[u8]) -> Option<&'static str> {
    if !bytes.starts_with(b"\x7fELF") {
        return None;
    }
    let machine = bytes.get(18..20)?;
    match u16::from_le_bytes([machine[0], machine[1]]) {
        0x3E => Some("x86_64"),
        0xB7 => Some("aarch64"),
        _ => None,
    }
}

/// Writes `bytes` with `mode` atomically: a sibling temp file, then a rename.
fn publish_file(path: &Path, bytes: &[u8], mode: u32) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let mut name = path
        .file_name()
        .ok_or_else(|| format!("{} has no file name", path.display()))?
        .to_owned();
    name.push(".tmp");
    let tmp = path.with_file_name(name);
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    };
    write().map_err(|e| format!("writing {}: {e}", path.display()))?;
    Ok(())
}
