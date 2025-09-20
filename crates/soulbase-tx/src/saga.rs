use crate::errors::TxError;
use crate::model::{SagaDefinition, SagaId, SagaInstance, SagaState, SagaStepStatus};
use crate::util::now_ms;
use async_trait::async_trait;
use rand::Rng;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait SagaStore: Send + Sync {
    async fn insert(&self, saga: SagaInstance) -> Result<SagaInstance, TxError>;
    async fn update(&self, saga: SagaInstance) -> Result<(), TxError>;
    async fn load(&self, id: &SagaId) -> Result<Option<SagaInstance>, TxError>;
}

#[async_trait]
pub trait SagaParticipant: Send + Sync {
    async fn execute(&self, uri: &str, saga: &SagaInstance) -> Result<bool, TxError>;
    async fn compensate(&self, uri: &str, saga: &SagaInstance) -> Result<bool, TxError>;
}

pub struct SagaOrchestrator<S, P>
where
    S: SagaStore,
    P: SagaParticipant,
{
    pub store: S,
    pub participant: P,
}

impl<S, P> SagaOrchestrator<S, P>
where
    S: SagaStore,
    P: SagaParticipant,
{
    pub async fn start(
        &self,
        tenant: &TenantId,
        definition: &SagaDefinition,
        created_at: Option<i64>,
    ) -> Result<SagaId, TxError> {
        let now = created_at.unwrap_or_else(now_ms);
        let id = SagaId(format!("{}-{}", tenant.0, random_suffix()));
        let instance = SagaInstance::new(id.clone(), tenant.clone(), definition.clone(), now);
        self.store.insert(instance).await?;
        Ok(id)
    }

    pub async fn tick(&self, id: &SagaId) -> Result<(), TxError> {
        let mut saga = match self.store.load(id).await? {
            Some(s) => s,
            None => return Err(TxError::not_found("saga not found")),
        };

        match saga.state {
            SagaState::Pending => {
                saga.state = SagaState::InProgress;
                saga.updated_at = now_ms();
                self.store.update(saga.clone()).await?;
                self.run_next_step(&mut saga, false).await?;
            }
            SagaState::InProgress => {
                self.run_next_step(&mut saga, false).await?;
            }
            SagaState::Compensating => {
                self.run_next_step(&mut saga, true).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn run_next_step(
        &self,
        saga: &mut SagaInstance,
        compensating: bool,
    ) -> Result<(), TxError> {
        if compensating {
            let idx_opt = saga
                .history
                .iter()
                .enumerate()
                .rev()
                .find(|(_, record)| matches!(record.status, SagaStepStatus::Executed));

            if let Some((idx, record)) = idx_opt {
                if let Some(uri) = saga.definition.steps[idx].compensate_uri.as_deref() {
                    let mut updated = record.clone();
                    match self.participant.compensate(uri, saga).await {
                        Ok(true) => {
                            let ts = now_ms();
                            updated.status = SagaStepStatus::Compensated;
                            updated.attempts += 1;
                            updated.last_updated = ts;
                            saga.history[idx] = updated;
                            saga.updated_at = ts;
                            if saga
                                .history
                                .iter()
                                .all(|r| !matches!(r.status, SagaStepStatus::Executed))
                            {
                                saga.state = SagaState::Cancelled;
                            }
                            self.store.update(saga.clone()).await?;
                        }
                        Ok(false) => {
                            updated.attempts += 1;
                            updated.last_updated = now_ms();
                            saga.history[idx] = updated;
                            saga.updated_at = now_ms();
                            self.store.update(saga.clone()).await?;
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                } else {
                    let ts = now_ms();
                    saga.history[idx].status = SagaStepStatus::Compensated;
                    saga.updated_at = ts;
                    if saga
                        .history
                        .iter()
                        .all(|r| !matches!(r.status, SagaStepStatus::Executed))
                    {
                        saga.state = SagaState::Cancelled;
                    }
                    self.store.update(saga.clone()).await?;
                }
            } else {
                let ts = now_ms();
                saga.state = SagaState::Cancelled;
                saga.updated_at = ts;
                self.store.update(saga.clone()).await?;
            }
        } else {
            if saga.current_step >= saga.definition.steps.len() {
                saga.state = SagaState::Completed;
                saga.updated_at = now_ms();
                self.store.update(saga.clone()).await?;
                return Ok(());
            }

            let idx = saga.current_step;
            let step_def = &saga.definition.steps[idx];
            let mut record = saga.history[idx].clone();
            record.attempts += 1;
            record.last_updated = now_ms();

            match self.participant.execute(&step_def.action_uri, saga).await {
                Ok(true) => {
                    record.status = SagaStepStatus::Executed;
                    saga.history[idx] = record;
                    saga.current_step += 1;
                    saga.updated_at = now_ms();
                    if saga.current_step >= saga.definition.steps.len() {
                        saga.state = SagaState::Completed;
                    }
                    self.store.update(saga.clone()).await?;
                }
                Ok(false) => {
                    record.status = SagaStepStatus::Failed("participant returned false".into());
                    saga.history[idx] = record;
                    saga.state = SagaState::Compensating;
                    saga.updated_at = now_ms();
                    self.store.update(saga.clone()).await?;
                }
                Err(err) => {
                    let err_obj = err.into_inner();
                    let err_msg = err_obj
                        .message_dev
                        .clone()
                        .unwrap_or_else(|| err_obj.message_user.clone());
                    record.status = SagaStepStatus::Failed(err_msg);
                    saga.history[idx] = record;
                    saga.state = SagaState::Compensating;
                    saga.updated_at = now_ms();
                    self.store.update(saga.clone()).await?;
                }
            }
        }
        Ok(())
    }
}

fn random_suffix() -> String {
    let mut rng = rand::thread_rng();
    format!("{:016x}", rng.gen::<u64>())
}
