use crate::errors::TxError;
use crate::model::DeadLetterRef;
use crate::outbox::{DeadStore, OutboxStore};
use soulbase_types::prelude::TenantId;

pub struct ReplayService<S, D>
where
    S: OutboxStore,
    D: DeadStore,
{
    outbox: S,
    dead: D,
}

impl<S, D> ReplayService<S, D>
where
    S: OutboxStore,
    D: DeadStore,
{
    pub fn new(outbox: S, dead: D) -> Self {
        Self { outbox, dead }
    }

    pub async fn replay(&self, reference: &DeadLetterRef) -> Result<(), TxError> {
        let tenant = &reference.tenant;
        let id = &reference.id;
        let letter = self.dead.load(tenant, id).await?;
        if letter.is_none() {
            return Err(TxError::not_found("dead-letter not found"));
        }
        self.outbox.requeue(tenant, id).await?;
        self.dead.delete(tenant, id).await?;
        Ok(())
    }
}

pub async fn replay_all<S, D>(
    service: &ReplayService<S, D>,
    tenant: &TenantId,
    ids: &[DeadLetterRef],
) -> Result<(), TxError>
where
    S: OutboxStore,
    D: DeadStore,
{
    for reference in ids.iter().filter(|r| &r.tenant == tenant) {
        service.replay(reference).await?;
    }
    Ok(())
}
