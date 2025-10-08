#[cfg(feature = "service")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    soulbase_llm::service::run().await
}

#[cfg(not(feature = "service"))]
fn main() {
    panic!("Enable the `service` feature to build the soulbase-llm-service binary");
}
