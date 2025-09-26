#[derive(Clone, Debug)]
pub struct BucketPolicy {
    pub default_private: bool,
    pub versioning: bool,
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self {
            default_private: true,
            versioning: false,
        }
    }
}
