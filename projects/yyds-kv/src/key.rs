use yyds_types::Namespace;

/// Logical key inside one shard. Namespaces partition tenant data without SQL schemas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    /// Tenant or table-group boundary.
    pub namespace: Namespace,
    /// Opaque key bytes inside the namespace.
    pub bytes: Vec<u8>,
}

impl Key {
    /// Builds a key from a namespace label and opaque bytes.
    pub fn new(namespace: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self { namespace: Namespace(namespace.into()), bytes: bytes.into() }
    }

    /// Stable diagnostic label for errors and logs.
    pub fn display(&self) -> String {
        format!("{}/{}", self.namespace.0, hex_preview(&self.bytes))
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    const LIMIT: usize = 16;
    let head = bytes.iter().take(LIMIT).map(|b| format!("{b:02x}")).collect::<String>();
    if bytes.len() > LIMIT {
        format!("{head}…({} bytes)", bytes.len())
    }
    else {
        head
    }
}
