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

use auth::entities::surreal::account::{AccountRole, CreateAccount, FindAccountByEmail};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use clap::{Parser, Subcommand};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::surreal::ca::{FindInternalCa, ListRelayCertificatesByPods};
use orchestration::entities::surreal::certificate::ListCertificatesBySnis;
use orchestration::services::OrchestrationError;
use orchestration::services::ca::{CaService, InitInternalCa};
use orchestration::services::derive::{
    DerivationCertificates, derive_server_config, relay_tls_pods, tls_snis,
};
use orchestration::utils::secret::SecretKey;
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
    /// Orchestration maintenance.
    Orchestration {
        #[command(subcommand)]
        command: OrchestrationCommand,
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
        Command::Orchestration {
            command: OrchestrationCommand::ExportConfig { server },
        } => export_config(SurrealProcessor::new(db), server).await,
        Command::Orchestration {
            command: OrchestrationCommand::InitCa,
        } => init_ca(SurrealProcessor::new(db)).await,
    }
}

/// Generates the internal CA. Every canvas tree holding a TLS/QUIC relay is
/// marked for re-derivation so its pods get their first leaf certificates.
async fn init_ca(db: SurrealProcessor) -> Result<(), Box<dyn std::error::Error>> {
    let ca = CaService {
        db,
        secrets: SecretKey::from_env()?,
        config: OrchestrationConfig::default(),
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
    let derived = derive_server_config(
        &topology,
        &server_id,
        &certificates,
        &OrchestrationConfig::default(),
    )?;
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
