use crate::model::{Cost, CostBreakdown, Usage};

/// 极简估算器（RIS）：tokens≈字符数/4；成本留空或可选返回零
pub fn estimate_usage(texts_in: &[&str], text_out: &str) -> Usage {
    let input_tokens: u32 = texts_in
        .iter()
        .map(|t| (t.chars().count() as u32).div_ceil(4))
        .sum();
    let output_tokens: u32 = (text_out.chars().count() as u32).div_ceil(4);
    Usage {
        input_tokens,
        output_tokens,
        cached_tokens: None,
        image_units: None,
        audio_seconds: None,
        requests: 1,
    }
}

pub fn zero_cost() -> Option<Cost> {
    Some(Cost {
        usd: 0.0,
        currency: "USD",
        breakdown: CostBreakdown {
            input: 0.0,
            output: 0.0,
            image: 0.0,
            audio: 0.0,
        },
    })
}
