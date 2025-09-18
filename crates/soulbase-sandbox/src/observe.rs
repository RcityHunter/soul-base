use crate::model::{Capability, ExecOp};
use std::collections::BTreeMap;

pub fn labels(
    tenant: &str,
    capability: Option<&Capability>,
    op: &ExecOp,
) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.to_string());
    map.insert("op", op.kind_name().to_string());
    if let Some(cap) = capability {
        map.insert("capability", capability_name(cap).to_string());
    }
    map
}

fn capability_name(cap: &Capability) -> &'static str {
    match cap {
        Capability::FsRead { .. } => "fs_read",
        Capability::FsWrite { .. } => "fs_write",
        Capability::FsList { .. } => "fs_list",
        Capability::NetHttp { .. } => "net_http",
        Capability::TmpUse => "tmp_use",
    }
}
