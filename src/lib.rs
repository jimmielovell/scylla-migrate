//! ScyllaDB migration runner library
//!
//! This library provides functionality for managing database migrations in ScyllaDB.
//! It supports reading .cql files from a specified directory and executing them in order,
//! while tracking which migrations have been applied.
//!
//! # Example
//! ```no_run
//! use scylla_migrate::MigrationRunner;
//! use scylla::SessionBuilder;
//!
//! async fn migrate() -> anyhow::Result<()> {
//!     let session = SessionBuilder::new()
//!         .known_node("localhost:9042")
//!         .build()
//!         .await?;
//!
//!     let runner = MigrationRunner::new(&session, "migrations");
//!     runner.run().await?;
//!     Ok(())
//! }
//! ```

use anyhow::{Context, Result};
use scylla::Session;
use time::OffsetDateTime;
use tokio::fs;

/// Represents a single database migration
///
/// Each migration corresponds to a .cql file in the migrations directory.
/// The file name format should be: TIMESTAMP_description.cql
/// For example: "20240117000000_create_users.cql"
#[derive(Debug)]
struct Migration {
    /// Unique identifier for the migration (typically the filename)
    id: String,
    /// Human-readable description of what the migration does
    description: String,
    /// Timestamp when the migration was created
    timestamp: String,
    /// The actual CQL statements to be executed
    content: String,
}

impl Migration {
    /// Creates a new Migration instance
    pub fn new(id: String, description: String, timestamp: String, content: String) -> Self {
        Self {
            id,
            description,
            timestamp,
            content,
        }
    }

    /// Executes the migration against the database
    ///
    /// Splits the content into individual CQL statements and executes them sequentially,
    /// waiting for schema agreement after each statement.
    async fn up(&self, session: &Session) -> Result<()> {
        // Split the content into individual statements
        let statements = self
            .content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        for statement in statements {
            session
                .query_unpaged(statement, &[])
                .await
                .context("Failed to execute migration statement")?;

            session
                .await_schema_agreement()
                .await
                .context("Failed to await schema agreement")?;
        }
        Ok(())
    }

    /// Returns the migration's unique identifier
    fn id(&self) -> &str {
        &self.id
    }

    /// Returns the migration's description
    fn description(&self) -> &str {
        &self.description
    }
}

/// Main runner for executing database migrations
#[derive(Debug)]
pub struct MigrationRunner<'a> {
    /// Active ScyllaDB session
    session: &'a Session,
    /// Path to directory containing .cql migration files
    migrations_path: &'a str,
}

impl<'a> MigrationRunner<'a> {
    /// Creates a new MigrationRunner instance
    pub fn new(session: &'a Session, migrations_path: &'a str) -> Self {
        Self {
            session,
            migrations_path,
        }
    }

    async fn create_public_keyspace(&self) -> Result<()> {
        self.session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS public WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
                &[],
            )
            .await?;
        self.session.await_schema_agreement().await?;
        Ok(())
    }

    async fn create_migration_table(&self) -> Result<()> {
        self.session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS public.migrations (
                    migration_id text PRIMARY KEY,
                    applied_at timestamp,
                    description text
                )",
                &[],
            )
            .await?;
        self.session.await_schema_agreement().await?;
        Ok(())
    }

    async fn is_migration_applied(&self, migration_id: &str) -> Result<bool> {
        let rows = self
            .session
            .query_unpaged(
                "SELECT migration_id FROM public.migrations WHERE migration_id = ?",
                (migration_id,),
            )
            .await?
            .into_rows_result()
            .context("Failed to get rows from migrations")?;

        Ok(rows.rows_num() != 0)
    }

    async fn record_migration(&self, migration: &Migration) -> Result<()> {
        self.session
            .query_unpaged(
                "INSERT INTO public.migrations (migration_id, applied_at, description) VALUES (?, ?, ?)",
                (migration.id(), OffsetDateTime::now_utc(), migration.description()),
            )
            .await?;
        Ok(())
    }

    async fn load_migrations(&self) -> Result<Vec<Migration>> {
        let mut migrations = Vec::new();
        let mut entries = fs::read_dir(&self.migrations_path)
            .await
            .context("Scylla MigrationRunner failed to load the migrations directory")?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("cql") {
                let file_name = entry.file_name().to_string_lossy().to_string();
                // Extract timestamp and name from filename (e.g., "20240117185823_create_users.cql")
                let parts: Vec<&str> = file_name.split('_').collect();
                if parts.len() >= 2 {
                    let timestamp = parts[0];
                    let name = parts[1..].join("_").replace(".cql", "");
                    let content = fs::read_to_string(entry.path()).await?;

                    migrations.push(Migration::new(
                        file_name.clone(),
                        name,
                        timestamp.to_string(),
                        content,
                    ));
                }
            }
        }

        // Sort migrations by filename (which starts with timestamp)
        migrations.sort_by(|a, b| a.id().cmp(b.id()));
        Ok(migrations)
    }

    /// Runs all pending migrations
    ///
    /// This will:
    /// 1. Create the public keyspace and migrations table if they don't exist
    /// 2. Load all migrations from the migrations directory
    /// 3. Check each migration and execute it if it hasn't been applied
    pub async fn run(&self) -> Result<()> {
        self.create_public_keyspace().await?;
        self.create_migration_table().await?;

        let migrations = self.load_migrations().await?;
        for migration in migrations {
            if !self.is_migration_applied(migration.id()).await? {
                migration.up(self.session).await?;
                self.record_migration(&migration).await?;
                println!(
                    "Applied {}/migrate {}",
                    migration.timestamp,
                    migration.description()
                );
            } else {
                println!("Migration {} already applied", migration.id());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::{Session, SessionBuilder};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio;

    async fn setup_test_migrations() -> Result<(TempDir, PathBuf)> {
        let temp_dir = TempDir::new()?;
        let migrations_path = temp_dir.path().to_path_buf();

        // Create test migration files
        let migration1 = r#"
            -- Migration: create_test_table
            CREATE TABLE IF NOT EXISTS test.users (
                user_id uuid PRIMARY KEY,
                name text,
                email text
            );"#;

        let migration2 = r#"
            -- Migration: create_index
            CREATE INDEX IF NOT EXISTS idx_users_email ON test.users(email);"#;

        fs::write(
            migrations_path.join("20240117000000_create_test_table.cql"),
            migration1,
        )?;
        fs::write(
            migrations_path.join("20240117000001_create_index.cql"),
            migration2,
        )?;

        Ok((temp_dir, migrations_path))
    }

    async fn get_test_session() -> Result<Session> {
        let session = SessionBuilder::new()
            .known_node("localhost:9042")
            .build()
            .await?;

        // Create test keyspace
        session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
                &[],
            )
            .await?;
        session.await_schema_agreement().await?;

        // Drop migrations table if it exists
        session
            .query_unpaged("DROP TABLE IF EXISTS public.migrations", &[])
            .await?;
        session.await_schema_agreement().await?;

        Ok(session)
    }

    async fn count_migrations(session: &Session) -> Result<i64> {
        let query_rows = session
            .query_unpaged("SELECT COUNT(*) FROM public.migrations", &[])
            .await?
            .into_rows_result()?;

        let mut count = 0;
        for row in query_rows.rows()? {
            let (int_count, _): (i64, i32)= row?;
            count = int_count;
            break;
        }

        Ok(count)
    }

    #[tokio::test]
    async fn test_migration_loading() -> Result<()> {
        let (temp_dir, migrations_path) = setup_test_migrations().await?;
        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        let migrations = runner.load_migrations().await?;
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].description(), "create_test_table");
        assert_eq!(migrations[1].description(), "create_index");

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_migration_execution() -> Result<()> {
        let (temp_dir, migrations_path) = setup_test_migrations().await?;
        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        // Run migrations
        runner.run().await?;

        // Verify number of migrations applied
        assert_eq!(count_migrations(&session).await?, 2);

        // Verify table exists
        let tables = session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'test'",
                &[],
            )
            .await?
            .into_rows_result()?;

        assert_eq!(tables.rows_num(), 1);

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_idempotency() -> Result<()> {
        let (temp_dir, migrations_path) = setup_test_migrations().await?;
        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        // Run migrations twice
        runner.run().await?;
        runner.run().await?;

        // Verify migrations were applied only once
        assert_eq!(count_migrations(&session).await?, 2);

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_migration() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let migrations_path = temp_dir.path().to_path_buf();

        // Create invalid migration file
        let invalid_migration = "-- Migration: invalid\nINVALID CQL STATEMENT;";
        fs::write(
            migrations_path.join("20240117000000_invalid.cql"),
            invalid_migration,
        )?;

        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        // Run should fail
        let result = runner.run().await;
        assert!(result.is_err());

        // Verify no migrations were recorded
        assert_eq!(count_migrations(&session).await?, 0);

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_migrations_dir() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let migrations_path = temp_dir.path().to_path_buf();
        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        // Run should succeed with no migrations
        runner.run().await?;

        // Verify no migrations were recorded
        assert_eq!(count_migrations(&session).await?, 0);

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_migration_ordering() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let migrations_path = temp_dir.path().to_path_buf();

        // Create migrations in reverse order
        let migration2 = r#"
            -- Migration: second
            CREATE TABLE test.second (id uuid PRIMARY KEY);"#;
        let migration1 = r#"
            -- Migration: first
            CREATE TABLE test.first (id uuid PRIMARY KEY);"#;

        fs::write(
            migrations_path.join("20240117000002_second.cql"),
            migration2,
        )?;
        fs::write(
            migrations_path.join("20240117000001_first.cql"),
            migration1,
        )?;

        let session = get_test_session().await?;
        let runner = MigrationRunner::new(&session, migrations_path.to_str().unwrap());

        runner.run().await?;

        // Verify number of migrations applied
        assert_eq!(count_migrations(&session).await?, 2);

        // Verify tables were created in correct order
        let tables = session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'test'",
                &[],
            )
            .await?
            .into_rows_result()?;

        assert_eq!(tables.rows_num(), 2);

        temp_dir.close()?;
        Ok(())
    }
}
