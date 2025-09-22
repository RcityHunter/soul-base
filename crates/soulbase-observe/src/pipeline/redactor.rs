pub trait Redactor: Send + Sync {
    fn redact_label(&self, key: &str, value: &str) -> String;
    fn redact_field(&self, key: &str, value: &str) -> String;
}

#[derive(Default)]
pub struct NoopRedactor;

impl Redactor for NoopRedactor {
    fn redact_label(&self, _key: &str, value: &str) -> String {
        value.to_string()
    }

    fn redact_field(&self, _key: &str, value: &str) -> String {
        value.to_string()
    }
}
