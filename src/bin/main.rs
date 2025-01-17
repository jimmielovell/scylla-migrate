use anyhow::Result;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;
use scylla::SessionBuilder;
use time::OffsetDateTime;
use scylla_migrate::MigrationRunner;

#[derive(Parser)]
#[command(name = "cargo")]
#[command(bin_name = "cargo")]
pub enum Cargo {
    #[command(name = "scylla-migrate")]
    ScyllaMigrate(ScyllaMigrateArgs),
}

#[derive(clap::Args)]
#[command(author, version, about)]
pub struct ScyllaMigrateArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Add a new migration
    Add {
        /// Name of the migration
        name: String,
        /// Directory to store migrations (optional)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },
    /// Run pending migrations
    Run {
        /// Directory containing migrations
        #[arg(short, long)]
        path: Option<PathBuf>,
        /// ScyllaDB connection string
        #[arg(short, long)]
        uri: String,
        /// ScyllaDB username (optional)
        #[arg(short, long)]
        user: Option<String>,
        /// ScyllaDB password (optional)
        #[arg(short, long)]
        password: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cargo::ScyllaMigrate(args) = Cargo::parse();

    match args.command {
        Commands::Add { name, path } => {
            let migrations_path = path.unwrap_or_else(|| PathBuf::from("migrations"));
            create_migration(&migrations_path, &name)?;
        }
        Commands::Run { path, uri, user, password } => {
            let migrations_path = path.unwrap_or_else(|| PathBuf::from("migrations"));
            run_migrations(&uri, &migrations_path, user, password).await?;
            println!("Running migrations from {:?} using {}", migrations_path, uri);
        }
    }

    Ok(())
}

fn create_migration(migrations_path: &PathBuf, name: &str) -> Result<()> {
    fs::create_dir_all(migrations_path)?;

    let timestamp = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)?
        .replace([':', '-', '.'], "")
        .split('T')
        .next()
        .unwrap()
        .to_string();

    let filename = format!("{}_{}.cql", timestamp, name);
    let filepath = migrations_path.join(filename);

    let content = format!(
        "-- Migration: {}\n-- Timestamp: {}\n\n-- Add your CQL queries here\n",
        name, timestamp
    );

    fs::write(&filepath, content)?;
    println!("Created migration file: {:?}", filepath);

    Ok(())
}

async fn run_migrations(
    node: &String,
    migrations_path: &PathBuf,
    user: Option<String>,
    password: Option<String>,
) -> Result<()> {
    let mut builder = SessionBuilder::new().known_node(node);

    if let (Some(username), Some(pass)) = (user, password) {
        builder = builder.user(username, pass);
    }

    let session = builder.build().await?;

    // Migrate the scylla database
    let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());
    runner.run().await?;

    Ok(())
}
