use anyhow::{Context, Result};
use clap::Parser;
use scylla::SessionBuilder;
use scylla_migrate::Migrator;
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

// cargo invokes this binary as `scylla-migrate <args>`
#[derive(Debug, Parser)]
#[command(bin_name = "scylla-migrate")]
#[command(version, about, long_about = None)]
enum Args {
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
    let args = Args::parse();

    match args {
        Args::Add { name, path } => {
            let migrations_path = path.unwrap_or_else(|| PathBuf::from("migrations"));
            create_migration(&migrations_path, &name)?;
        }
        Args::Run {
            path,
            uri,
            user,
            password,
        } => {
            let migrations_path = path.unwrap_or_else(|| PathBuf::from("migrations"));
            run_migrations(&uri, &migrations_path, user, password).await?;
        }
    }

    Ok(())
}

fn create_migration(migrations_path: &PathBuf, name: &str) -> Result<()> {
    fs::create_dir_all(migrations_path).context("Unable to create migrations directory")?;

    let dt = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)?
        .replace([':', '-', '.'], "")
        .split('T')
        .next()
        .unwrap()
        .to_string();

    let filename = format!("{}_{}.cql", dt, name);
    let filepath = migrations_path.join(filename);

    let content = format!(
        "-- Migration: {}\n-- Timestamp: {}\n\n-- Add your CQL queries here\n",
        name, dt
    );

    fs::write(&filepath, content)?;
    println!("Created migration: {:?}", filepath);

    Ok(())
}

async fn run_migrations(
    node: &String,
    migrations_path: &Path,
    user: Option<String>,
    password: Option<String>,
) -> Result<()> {
    let mut builder = SessionBuilder::new().known_node(node);

    if let (Some(username), Some(pass)) = (user, password) {
        builder = builder.user(username, pass);
    }

    let session = builder.build().await?;

    // Migrate the scylla database
    let runner = Migrator::new(&session, migrations_path.to_str().unwrap());
    runner.run().await?;

    Ok(())
}
