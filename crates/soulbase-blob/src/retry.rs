use std::future::Future;
use std::time::Duration;

pub async fn with_retry<F, Fut, T, E>(mut op: F, retries: usize) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut last_err = None;
    for _ in 0..=retries {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Err(last_err.expect("retry attempts exhausted without error"))
}
