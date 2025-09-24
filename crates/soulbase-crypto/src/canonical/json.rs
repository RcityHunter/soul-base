use serde_json::Value;

use crate::errors::CryptoError;

pub fn canonicalize_to_string(value: &Value) -> Result<String, CryptoError> {
    let mut out = String::new();
    write_value(value, &mut out)?;
    Ok(out)
}

fn write_value(value: &Value, out: &mut String) -> Result<(), CryptoError> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(num) => {
            if num.is_f64() {
                return Err(CryptoError::canonical(
                    "floating point numbers are not allowed in canonical JSON",
                ));
            }
            out.push_str(&num.to_string());
        }
        Value::String(s) => {
            out.push_str(&serde_json::to_string(s).map_err(|err| {
                CryptoError::canonical(&format!("string escaping failed: {err}"))
            })?);
        }
        Value::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_value(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (idx, (key, value)) in entries.into_iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).map_err(|err| {
                    CryptoError::canonical(&format!("key escaping failed: {err}"))
                })?);
                out.push(':');
                write_value(value, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}
