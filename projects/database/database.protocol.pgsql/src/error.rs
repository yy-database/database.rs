use core::fmt;

/// One field of an `ErrorResponse` or `NoticeResponse`, kept in wire order.
///
/// `kind` is the one-byte field selector from the protocol (`S`, `V`, `C`, `M`,
/// `D`, `H`, `P`, `s`, `t`, `c`, `n`, ...). Values are decoded exactly as the
/// server sent them, so a caller can compare them with the raw payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageField {
    pub kind: u8,
    pub value: String,
}

impl MessageField {
    pub fn new(kind: u8, value: impl Into<String>) -> Self {
        Self { kind, value: value.into() }
    }
}

/// Everything a protocol-v3 client can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresError {
    /// Socket level failure: refused, reset, closed early.
    Io(String),
    /// The server answered with an `ErrorResponse`.
    Server {
        code: String,
        message: String,
        /// Every field the server sent, in wire order.
        fields: Vec<MessageField>,
    },
    /// The byte stream did not match the protocol contract.
    Protocol(String),
    /// A feature this client deliberately refuses instead of degrading.
    Unsupported(String),
}

impl PostgresError {
    /// Server error code such as `42P01`. Absent for transport failures.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Server { code, .. } => Some(code),
            _ => None,
        }
    }

    /// Human readable message without the code prefix.
    pub fn message(&self) -> &str {
        match self {
            Self::Io(message) | Self::Protocol(message) | Self::Unsupported(message) => message,
            Self::Server { message, .. } => message,
        }
    }

    /// Every `ErrorResponse` field in wire order, empty for transport failures.
    pub fn fields(&self) -> &[MessageField] {
        match self {
            Self::Server { fields, .. } => fields,
            _ => &[],
        }
    }

    /// One `ErrorResponse` field by selector, such as `b'P'` for position.
    pub fn field(&self, kind: u8) -> Option<&str> {
        self.fields().iter().find(|field| field.kind == kind).map(|field| field.value.as_str())
    }

    /// Feature name of an [`PostgresError::Unsupported`] rejection.
    pub fn feature(&self) -> Option<&str> {
        match self {
            Self::Unsupported(feature) => Some(feature),
            _ => None,
        }
    }
}

impl fmt::Display for PostgresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) | Self::Protocol(message) => formatter.write_str(message),
            Self::Server { code, message, .. } => write!(formatter, "{code}: {message}"),
            Self::Unsupported(feature) => write!(formatter, "unsupported: {feature}"),
        }
    }
}

impl std::error::Error for PostgresError {}

/// One `NoticeResponse` the server sent while a statement ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// Every field the server sent, in wire order.
    pub fields: Vec<MessageField>,
}

impl Notice {
    pub fn new(fields: Vec<MessageField>) -> Self {
        Self { fields }
    }

    /// One field by selector, such as `b'C'` for the SQLSTATE code.
    pub fn field(&self, kind: u8) -> Option<&str> {
        self.fields.iter().find(|field| field.kind == kind).map(|field| field.value.as_str())
    }

    /// `S`: localized severity, `NOTICE` or `WARNING`.
    pub fn severity(&self) -> Option<&str> {
        self.field(b'S')
    }

    /// `C`: SQLSTATE code.
    pub fn code(&self) -> Option<&str> {
        self.field(b'C')
    }

    /// `M`: message text.
    pub fn message(&self) -> Option<&str> {
        self.field(b'M')
    }
}
