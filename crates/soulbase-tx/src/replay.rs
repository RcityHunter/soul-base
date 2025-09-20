use crate::errors::TxError;
use crate::model::{DeadKind, DeadLetterRef};
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
        if reference.kind != DeadKind::Outbox {
            return Err(TxError::bad_request(
                "only outbox dead letters supported for replay",
            ));
        }
        let Some(letter) = self.dead.inspect(reference).await? else {
            return Err(TxError::not_found("dead-letter not found"));
        };
        self.outbox
            .requeue(&reference.tenant, &reference.id)
            .await?;
        self.dead.delete(&letter.reference).await?;
        Ok(())
    }
}

pub async fn replay_all<S, D>(
    service: &ReplayService<S, D>,
    tenant: &TenantId,
    refs: &[DeadLetterRef],
) -> Result<(), TxError>
where
    S: OutboxStore,
    D: DeadStore,
{
    for reference in refs.iter().filter(|r| &r.tenant == tenant) {
        service.replay(reference).await?;
    }
    Ok(())
}
