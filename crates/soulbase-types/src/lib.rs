pub mod id;
pub mod time;
pub mod tenant;
pub mod subject;
pub mod scope;
pub mod trace;
pub mod envelope;
pub mod traits;
pub mod validate;
pub mod prelude;

#[cfg(feature = "schema")]
pub mod schema_gen {
    use super::*;
    use schemars::schema::RootSchema;
    use schemars::schema_for;

    pub fn envelope_schema<T>() -> RootSchema
    where
        T: schemars::JsonSchema,
    {
        schema_for!(envelope::Envelope<T>)
    }
}
