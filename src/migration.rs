use anyhow::{Context, Result};
use scylla::Session;
use sha2::{Digest, Sha384};
use std::borrow::Cow;

/// Represents a single database migration
///
/// Each migration corresponds to a .cql file in the migrations directory.
/// The file name format should be: TIMESTAMP_description.cql
/// For example: "20240117000000_create_users.cql"
#[derive(Debug)]
pub struct Migration {
    pub version: i64,
    pub description: Cow<'static, str>,
    pub cql: Cow<'static, str>,
    pub checksum: Cow<'static, [u8]>,
}

impl Migration {
    /// Creates a new Migration instance
    pub fn new(version: i64, description: Cow<'static, str>, cql: Cow<'static, str>) -> Self {
        let checksum = Cow::Owned(Vec::from(Sha384::digest(cql.as_bytes()).as_slice()));

        Migration {
            version,
            description,
            cql,
            checksum,
        }
    }

    pub async fn up(&self, session: &Session) -> Result<()> {
        // Split the content into individual statements
        let statements: Vec<_> = self
            .cql
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for stmt in statements {
            session
                .query_unpaged(stmt, &[])
                .await
                .with_context(|| format!("Failed to execute migration statement: {}", stmt))?;
        }

        Ok(())
    }
}

pub struct AppliedMigration {
    pub checksum: Cow<'static, [u8]>,
}
