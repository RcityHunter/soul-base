use soulbase_config::prelude::*;
use soulbase_config::source::{cli::CliArgsSource, env::EnvSource, file::FileSource};
use std::sync::Arc;

#[test]
fn load_minimal_snapshot_and_read() {
    let loader = Loader {
        sources: vec![
            Arc::new(FileSource { paths: vec![] }) as Arc<dyn Source>,
            Arc::new(EnvSource {
                prefix: "SOUL__".into(),
                separator: "__".into(),
            }) as Arc<dyn Source>,
            Arc::new(CliArgsSource {
                args: vec!["--app.name=Soulseed".into()],
            }) as Arc<dyn Source>,
        ],
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    };

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let snapshot = runtime
        .block_on(async { loader.load_once().await })
        .expect("snapshot");

    let app_name: String = snapshot.get(&KeyPath("app.name".into())).expect("app.name");
    assert_eq!(app_name, "Soulseed");
    assert!(!snapshot.checksum().0.is_empty());
}
