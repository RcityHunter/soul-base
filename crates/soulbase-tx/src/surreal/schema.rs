#![cfg(feature = "surreal")]

use sha2::{Digest, Sha256};
use soulbase_storage::prelude::MigrationScript;

pub const TABLE_OUTBOX: &str = "tx_outbox";
pub const TABLE_DEAD_LETTER: &str = "tx_dead_letter";
pub const TABLE_IDEMPO: &str = "tx_idempo";

const MIGRATION_VERSION_V1: &str = "0001_tx_outbox";

const MIGRATION_V1_UP: &str = r#"
DEFINE TABLE tx_outbox SCHEMAFULL;
DEFINE FIELD msg_id ON tx_outbox TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD tenant ON tx_outbox TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD channel ON tx_outbox TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD payload ON tx_outbox TYPE any;
DEFINE FIELD attempts ON tx_outbox TYPE int DEFAULT 0;
DEFINE FIELD status ON tx_outbox TYPE string ASSERT $value INSIDE ["Pending", "Leased", "Delivered", "Dead"];
DEFINE FIELD visible_at ON tx_outbox TYPE int;
DEFINE FIELD lease_worker ON tx_outbox TYPE option<string>;
DEFINE FIELD lease_until ON tx_outbox TYPE option<int>;
DEFINE FIELD last_error ON tx_outbox TYPE option<string>;
DEFINE FIELD dispatch_key ON tx_outbox TYPE option<string>;
DEFINE FIELD envelope_id ON tx_outbox TYPE option<string>;
DEFINE FIELD created_at ON tx_outbox TYPE int;
DEFINE FIELD updated_at ON tx_outbox TYPE int;
DEFINE INDEX uniq_tx_outbox_id ON TABLE tx_outbox FIELDS tenant, msg_id UNIQUE;
DEFINE INDEX idx_tx_outbox_ready ON TABLE tx_outbox FIELDS tenant, status, visible_at;
DEFINE INDEX idx_tx_outbox_dispatch_key ON TABLE tx_outbox FIELDS tenant, dispatch_key;

DEFINE TABLE tx_dead_letter SCHEMAFULL;
DEFINE FIELD letter_id ON tx_dead_letter TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD tenant ON tx_dead_letter TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD kind ON tx_dead_letter TYPE string ASSERT $value INSIDE ["Outbox", "Saga"];
DEFINE FIELD last_error ON tx_dead_letter TYPE option<string>;
DEFINE FIELD note ON tx_dead_letter TYPE option<string>;
DEFINE FIELD stored_at ON tx_dead_letter TYPE int;
DEFINE INDEX uniq_tx_dead_letter_id ON TABLE tx_dead_letter FIELDS tenant, kind, letter_id UNIQUE;
DEFINE INDEX idx_tx_dead_letter_lookup ON TABLE tx_dead_letter FIELDS tenant, kind;

DEFINE TABLE tx_idempo SCHEMAFULL;
DEFINE FIELD tenant ON tx_idempo TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD key ON tx_idempo TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD hash ON tx_idempo TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD state ON tx_idempo TYPE string ASSERT $value INSIDE ["InFlight", "Succeeded", "Failed"];
DEFINE FIELD digest ON tx_idempo TYPE option<string>;
DEFINE FIELD error ON tx_idempo TYPE option<string>;
DEFINE FIELD expires_at ON tx_idempo TYPE option<int>;
DEFINE FIELD updated_at ON tx_idempo TYPE int;
DEFINE INDEX uniq_tx_idempo_key ON TABLE tx_idempo FIELDS tenant, key UNIQUE;
DEFINE INDEX idx_tx_idempo_expiry ON TABLE tx_idempo FIELDS tenant, expires_at;
"#;

const MIGRATION_V1_DOWN: &str = r#"
REMOVE TABLE tx_idempo;
REMOVE TABLE tx_dead_letter;
REMOVE TABLE tx_outbox;
"#;

pub fn initial_migration() -> MigrationScript {
    MigrationScript {
        version: MIGRATION_VERSION_V1.to_string(),
        up_sql: MIGRATION_V1_UP.trim().to_string(),
        down_sql: MIGRATION_V1_DOWN.trim().to_string(),
        checksum: checksum(MIGRATION_V1_UP, MIGRATION_V1_DOWN),
    }
}

pub fn migrations() -> Vec<MigrationScript> {
    vec![initial_migration()]
}

fn checksum(up: &str, down: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(up.as_bytes());
    hasher.update([0x00]);
    hasher.update(down.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_is_stable() {
        let script = initial_migration();
        assert!(script.checksum.starts_with("sha256:"));
        assert!(script.up_sql.contains("DEFINE TABLE tx_outbox"));
        assert!(script.up_sql.contains("DEFINE TABLE tx_idempo"));
        assert!(script.down_sql.contains("REMOVE TABLE tx_outbox"));
    }
}
