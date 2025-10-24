use clap::{ArgAction, Args, Parser, Subcommand};
use serde::Serialize;
use serde_json::Value;
use soulbase_config::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(
    name = "soulbase-configctl",
    about = "Schema export & validation utilities for Soulbase configuration."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Schema(SchemaArgs),
    Validate(ValidateArgs),
}

#[derive(Debug, Args)]
struct SchemaArgs {
    #[arg(long, value_name = "FILE")]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct ValidateArgs {
    #[arg(long = "file", value_name = "PATH", action = ArgAction::Append)]
    files: Vec<PathBuf>,

    #[arg(long, default_value_t = false)]
    include_env: bool,

    #[arg(long, default_value = "SB")]
    env_prefix: String,

    #[arg(long, default_value = "__")]
    env_separator: String,

    #[arg(long, value_name = "JSON")]
    overrides: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    snapshot_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    provenance_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    changes_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    baseline: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Schema(args) => run_schema(args).await?,
        Command::Validate(args) => run_validate(args).await?,
    }
    Ok(())
}

async fn run_schema(args: SchemaArgs) -> Result<(), Box<dyn Error>> {
    let handles = bootstrap_catalog().await?;
    let docs = export_namespace_docs(&handles.registry).await?;
    write_json(&args.output, &docs)?;
    println!(
        "Exported {} namespaces to {}",
        docs.len(),
        args.output.display()
    );
    Ok(())
}

async fn run_validate(args: ValidateArgs) -> Result<(), Box<dyn Error>> {
    let handles = bootstrap_catalog().await?;
    let validator = handles.validator.clone();

    let mut builder = Loader::builder()
        .with_registry(handles.registry.clone())
        .with_validator(validator_as_trait(&validator));

    if !args.files.is_empty() {
        builder = builder.add_file_sources(args.files.clone());
    }

    if args.include_env {
        builder = builder.add_env_source(args.env_prefix.clone(), args.env_separator.clone());
    }

    let loader = Arc::new(builder.build());

    let snapshot = if let Some(path) = args.overrides.as_ref() {
        let overrides = read_json(path)?;
        loader.load_with(overrides).await?
    } else {
        loader.load_once().await?
    };

    let baseline = if let Some(path) = args.baseline.as_ref() {
        Some(read_json(path)?)
    } else {
        None
    };

    if let Some(ref previous) = baseline {
        validator.validate_delta(previous, snapshot.tree()).await?;
    }

    if let Some(path) = args.snapshot_out.as_ref() {
        write_json(path, snapshot.tree())?;
    }

    if let Some(path) = args.provenance_out.as_ref() {
        write_json(path, snapshot.provenance())?;
    }

    let changes = baseline
        .as_ref()
        .map(|value| diff_snapshot(value, snapshot.tree()))
        .unwrap_or_default();

    if let Some(path) = args.changes_out.as_ref() {
        write_json(path, &changes)?;
    }

    println!("snapshot={} diff={}", snapshot.checksum().0, changes.len());

    Ok(())
}

fn read_json(path: &Path) -> Result<Value, Box<dyn Error>> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

fn write_json<T: Serialize>(path: &Path, value: T) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, &value)?;
    writer.flush()?;
    Ok(())
}
