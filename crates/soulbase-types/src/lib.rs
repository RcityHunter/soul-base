pub mod envelope;
pub mod id;
pub mod prelude;
pub mod scope;
pub mod subject;
pub mod tenant;
pub mod time;
pub mod trace;
pub mod traits;
pub mod validate;

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
