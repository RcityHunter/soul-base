#![cfg(feature = "surreal")]

use clap::{Parser, Subcommand};
use soulbase_storage::spi::migrate::MigrationScript;
use soulbase_storage::surreal::{
    schema_migrations, SurrealConfig, SurrealDatastore, SurrealMigrator,
};
use soulbase_storage::Migrator;
use soulbase_storage::StorageError;
use std::collections::HashSet;

#[derive(Debug, Parser)]
#[command(
    name = "soulbase-storage-migrate",
    about = "Apply Soulbase SurrealDB schema migrations",
    version,
    author
)]
struct Cli {
    /// SurrealDB endpoint (e.g. mem:// or http://127.0.0.1:8000)
    #[arg(long, default_value = "mem://")]
    endpoint: String,

    /// SurrealDB namespace
    #[arg(long, default_value = "soul")]
    namespace: String,

    /// SurrealDB database
    #[arg(long, default_value = "default")]
    database: String,

    /// SurrealDB root username
    #[arg(long)]
    username: Option<String>,

    /// SurrealDB root password
    #[arg(long)]
    password: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Show applied and pending migration versions
    Status,
    /// Apply pending migrations (optionally up to a target version)
    Up {
        /// Target migration version to apply up to (inclusive)
        #[arg(long)]
        to: Option<String>,

        /// Show planned migrations without executing them
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), StorageError> {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Up {
        to: None,
        dry_run: false,
    });

    let mut config = SurrealConfig::new(cli.endpoint, cli.namespace, cli.database);
    match (cli.username, cli.password) {
        (Some(user), Some(pass)) => {
            config = config.with_credentials(user, pass);
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(StorageError::bad_request(
                "both username and password are required when specifying credentials",
            ));
        }
        (None, None) => {}
    }

    let datastore = SurrealDatastore::connect(config).await?;
    let migrator = datastore.migrator();

    let mut scripts = schema_migrations();
    scripts.sort_by(|a, b| a.version.cmp(&b.version));

    match command {
        Command::Status => status(&migrator, &scripts).await,
        Command::Up { to, dry_run } => up(&migrator, &scripts, to.as_deref(), dry_run).await,
    }
}

async fn status(
    migrator: &SurrealMigrator,
    scripts: &[MigrationScript],
) -> Result<(), StorageError> {
    let applied = migrator.applied_versions().await?;
    let current = applied
        .last()
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    println!("Current version: {current}");

    if applied.is_empty() {
        println!("Applied migrations: (none)");
    } else {
        println!("Applied migrations:");
        for version in &applied {
            println!("- {version}");
        }
    }

    let applied_set: HashSet<String> = applied.into_iter().collect();
    let pending: Vec<_> = scripts
        .iter()
        .filter(|script| !applied_set.contains(&script.version))
        .map(|script| script.version.clone())
        .collect();

    if pending.is_empty() {
        println!("Pending migrations: (none)");
    } else {
        println!("Pending migrations:");
        for version in pending {
            println!("- {version}");
        }
    }

    Ok(())
}

async fn up(
    migrator: &SurrealMigrator,
    scripts: &[MigrationScript],
    target: Option<&str>,
    dry_run: bool,
) -> Result<(), StorageError> {
    let applied = migrator.applied_versions().await?;
    let applied_set: HashSet<String> = applied.iter().cloned().collect();

    let mut pending: Vec<_> = scripts
        .iter()
        .filter(|script| !applied_set.contains(&script.version))
        .cloned()
        .collect();

    if let Some(target_version) = target {
        if !scripts
            .iter()
            .any(|script| script.version == target_version)
        {
            return Err(StorageError::bad_request(
                "target version not found in migration set",
            ));
        }
        pending.retain(|script| script.version.as_str() <= target_version);
    }

    if pending.is_empty() {
        println!("No pending migrations. Surreal schema is up to date.");
        return Ok(());
    }

    println!("Pending migrations to apply:");
    for script in &pending {
        println!("- {}", script.version);
    }

    if dry_run {
        println!("Dry run complete. No migrations were applied.");
        return Ok(());
    }

    migrator.apply_up(&pending).await?;
    println!("Applied {} migration(s).", pending.len());

    Ok(())
}
