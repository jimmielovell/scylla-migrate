# scylla-migrate

A Rust library and CLI tool for managing ScyllaDB migrations. This project provides both a programmatic API for managing migrations in your Rust applications and a command-line interface for manual migration management.

## Features

- Simple and intuitive migration file format (CQL)
- Timestamps-based migration ordering
- Both library and CLI interfaces
- Safe migration application (runs migrations exactly once)
- Supports custom migration directories

## Installation

### As a CLI Tool

```bash
cargo install scylla-migrate
```

### As a Library

Add this to your `Cargo.toml`:

```toml
[dependencies]
scylla-migrate = "0.1.0"
```

## Usage

### Command Line Interface

#### Creating a New Migration

```bash
# Create a migration in the default directory (./migrations)
cargo scylla-migrate add create_users

# Create a migration in a custom directory
cargo scylla-migrate add create_users --path ./my-migrations
```

This will create a new file with a name like `20240117000000_create_users.cql` containing:

```sql
-- Migration: create_users
-- Timestamp: 20240117000000

-- Add your CQL queries here
```

#### Running Migrations

```bash
# Run migrations from default directory
cargo scylla-migrate run --uri "scylla://localhost:9042"

# Run migrations from custom directory
cargo scylla-migrate run --path ./my-migrations --uri "scylla://localhost:9042"

# Run migrations with authentication
cargo scylla-migrate run \
    --uri "scylla://localhost:9042" \
    --user myuser \
    --password mypassword
```

### Library Usage

```rust
use scylla::SessionBuilder;
use scylla_migrate::MigrationRunner;

async fn migrate_database() -> Result<(), Box<dyn std::error::Error>> {
    // Create ScyllaDB session
    let session = SessionBuilder::new()
        .known_node("localhost:9042")
        .user("username", "password") // Optional authentication
        .build()
        .await?;

    // Create and run the migration runner
    let runner = MigrationRunner::new(&session, "migrations");
    runner.run().await?;

    Ok(())
}
```

## Migration Files

Migration files are plain `.cql` files containing ScyllaDB CQL statements. Multiple statements in a single file should be separated by semicolons. Example:

```sql
-- Migration: create_users
-- Timestamp: 20240117000000

CREATE TYPE IF NOT EXISTS user_status (
    value text
);

CREATE TABLE IF NOT EXISTS users (
    user_id uuid,
    email text,
    status frozen<user_status>,
    created_at timestamp,
    PRIMARY KEY (user_id)
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

## Migration Tracking

Migrations are tracked in a `public.migrations` table in your ScyllaDB instance. The schema for this table is:

```sql
CREATE TABLE public.migrations (
    migration_id text PRIMARY KEY,
    applied_at timestamp,
    description text
);
```

Each migration is run exactly once, and subsequent runs will skip already-applied migrations.

## Command Line Options

### Add Command
```bash
cargo scylla-migrate add <name> [OPTIONS]

OPTIONS:
    -p, --path <path>    Custom directory for migrations
```

### Run Command
```bash
cargo scylla-migrate run [OPTIONS]

OPTIONS:
    -p, --path <path>          Custom directory for migrations
    -u, --uri <uri>            ScyllaDB connection string
    -u, --user <username>      ScyllaDB username (optional)
    -p, --password <password>  ScyllaDB password (optional)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Development

### Building from Source

```bash
git clone https://github.com/jimmielovell/scylla-migrate
cd scylla-migrate
cargo build
```

## Known Limitations

- No support for migration rollbacks

## Roadmap

- [ ] Add support for migration rollbacks
- [ ] Add dry-run mode
- [ ] Add support for environment variables
