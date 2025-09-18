use crate::errors::LlmError;

#[derive(Clone, Debug)]
pub enum StructOutPolicy {
    Off,
    StrictReject,
    StrictRepair { max_attempts: u8 },
}

pub fn enforce_json(text: &str, policy: &StructOutPolicy) -> Result<serde_json::Value, LlmError> {
    match policy {
        StructOutPolicy::Off => serde_json::from_str(text)
            .map_err(|e| LlmError::schema(&format!("json parse (off policy): {e}"))),
        StructOutPolicy::StrictReject => {
            serde_json::from_str(text).map_err(|e| LlmError::schema(&format!("json parse: {e}")))
        }
        StructOutPolicy::StrictRepair { max_attempts } => {
            // 轻修复桩：尝试去除尾随反引号/代码块围栏
            let mut s = text.trim().to_string();
            let mut tries = 0u8;
            loop {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(v) => return Ok(v),
                    Err(_e) if tries < *max_attempts => {
                        tries += 1;
                        s = s.trim_matches('`').trim().to_string();
                    }
                    Err(e) => {
                        return Err(LlmError::schema(&format!("json parse after repair: {e}")))
                    }
                }
            }
        }
    }
}
