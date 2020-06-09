//! Normalized Iris values / rows (Phase 1).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

/// Tagged JSON key of the shared binary-payload wire shapes (`$database`).
///
/// The tag name selects the carrier: `bytes`, `vector` or `file`.
pub const TAGGED_JSON_KEY: &str = "$database";

/// Phase 1 value model (VOS-aligned subset, not a SQL value).
///
/// `PartialEq` / `Eq` / `Ord` are hand-written because the VOS `f64` carries a
/// native IEEE-754 double while the model still needs a total, hash-free
/// ordering: floats compare by [`f64::total_cmp`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Null.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    Int(i64),
    /// Unsigned 64-bit integer (VOS `u64`); the full range is preserved.
    UInt(u64),
    /// Finite IEEE-754 double (VOS `f64`); never silently degraded to text.
    Float(f64),
    /// UTF-8 string.
    Str(String),
    /// Exact decimal value in canonical text form.
    Decimal(String),
    /// Calendar date in canonical text form.
    Date(String),
    /// Wall-clock time in canonical text form.
    Time(String),
    /// UTC datetime in canonical text form.
    DateTime(String),
    /// Octets of the VOS `bytes` scalar, exactly as the backend produced them.
    Bytes(Vec<u8>),
    /// Fixed-dimension `f32` vector of the VOS `vector` scalar.
    Vector(VectorValue),
    /// File descriptor of the VOS `file` scalar: content identity, metadata
    /// and the immutable chunk manifest reference, as the backend's CAS
    /// object store produced it.
    File(FileDescriptor),
    /// Nested object row (query projection).
    Object(BTreeMap<String, Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::UInt(a), Self::UInt(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(b),
            (Self::Str(a), Self::Str(b))
            | (Self::Decimal(a), Self::Decimal(b))
            | (Self::Date(a), Self::Date(b))
            | (Self::Time(a), Self::Time(b))
            | (Self::DateTime(a), Self::DateTime(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            // Structural order (dimension, then bit patterns) gives sorting a
            // deterministic total order; it is *not* an equality semantics —
            // vector comparison for queries is similarity under an explicit
            // metric (see `VectorValue::similarity`).
            (Self::Vector(a), Self::Vector(b)) => a.cmp(b),
            // File identity is the content digest: the SHA-256 over the
            // complete octets. `name` / `media_type` are labels and never
            // take part in comparison, so equal digests mean the same file.
            (Self::File(a), Self::File(b)) => a.digest.cmp(&b.digest),
            (Self::Object(a), Self::Object(b)) => a.cmp(b),
            _ => Self::rank(self).cmp(&Self::rank(other)),
        }
    }
}

impl Value {
    /// Position of the variant in the model, used as the primary `Ord` key.
    fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool(_) => 1,
            Self::Int(_) => 2,
            Self::UInt(_) => 3,
            Self::Float(_) => 4,
            Self::Str(_) => 5,
            Self::Decimal(_) => 6,
            Self::Date(_) => 7,
            Self::Time(_) => 8,
            Self::DateTime(_) => 9,
            Self::Bytes(_) => 10,
            Self::Vector(_) => 11,
            Self::File(_) => 12,
            Self::Object(_) => 13,
        }
    }
}

/// Fixed-dimension vector of finite `f32` components (VOS `vector`).
///
/// `Deserialize` is hand-written so that JSON input cannot construct a vector
/// that [`VectorValue::new`] would reject: every deserialized value goes
/// through that single validation entry point.
#[derive(Debug, Clone, Serialize)]
pub struct VectorValue {
    /// Dimension fixed by the contract; equals `components.len()`.
    pub dimension: u32,
    /// Components in ascending index order.
    pub components: Vec<f32>,
}

impl VectorValue {
    /// Builds a vector, rejecting a zero dimension, a dimension/component-count
    /// mismatch and any non-finite component (the VOS contract only carries
    /// finite, non-empty embeddings).
    pub fn new(dimension: u32, components: Vec<f32>) -> Option<Self> {
        if dimension == 0 || dimension as usize != components.len() || components.iter().any(|c| !c.is_finite()) {
            return None;
        }
        Some(Self { dimension, components })
    }

    /// Little-endian `f32` octets — the canonical binary form shared with the
    /// YYDB `vector` payload and the tagged wire hex.
    pub fn to_le_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.components.len() * 4);
        for component in &self.components {
            out.extend_from_slice(&component.to_le_bytes());
        }
        out
    }

    /// Decodes little-endian `f32` octets into a vector; `dimension` must match
    /// the payload length and every component must be finite.
    pub fn from_le_bytes(dimension: u32, octets: &[u8]) -> Option<Self> {
        if octets.len() != dimension as usize * 4 {
            return None;
        }
        let components = octets.as_chunks::<4>().0.iter().map(|chunk| f32::from_le_bytes(*chunk)).collect::<Vec<_>>();
        Self::new(dimension, components)
    }

    /// Similarity or distance to `other` under an explicit `metric`.
    ///
    /// Vector comparison is a query capability, never a storage default: the
    /// caller must always pick the metric, and a dimension mismatch is
    /// rejected instead of silently producing a number. `Cosine` and
    /// `DotProduct` return scores where a higher value means closer, while
    /// `Euclidean` returns a distance where a lower value means closer; the
    /// query side owns the threshold direction (see
    /// [`similarity_meets_threshold`]).
    pub fn similarity(&self, other: &Self, metric: SimilarityMetric) -> Result<f64, VectorSimilarityError> {
        if self.dimension != other.dimension {
            return Err(VectorSimilarityError::DimensionMismatch);
        }
        match metric {
            SimilarityMetric::Cosine => {
                let mut dot = 0.0_f64;
                let mut left_norm = 0.0_f64;
                let mut right_norm = 0.0_f64;
                for (left, right) in self.components.iter().zip(&other.components) {
                    let left = f64::from(*left);
                    let right = f64::from(*right);
                    dot += left * right;
                    left_norm += left * left;
                    right_norm += right * right;
                }
                if left_norm == 0.0 || right_norm == 0.0 {
                    return Err(VectorSimilarityError::ZeroNorm);
                }
                Ok(dot / (left_norm.sqrt() * right_norm.sqrt()))
            }
            SimilarityMetric::DotProduct => {
                Ok(self.components.iter().zip(&other.components).map(|(left, right)| f64::from(*left) * f64::from(*right)).sum())
            }
            SimilarityMetric::Euclidean => {
                let squared: f64 = self
                    .components
                    .iter()
                    .zip(&other.components)
                    .map(|(left, right)| {
                        let delta = f64::from(*left) - f64::from(*right);
                        delta * delta
                    })
                    .sum();
                Ok(squared.sqrt())
            }
        }
    }
}

/// Explicit metric for vector similarity queries.
///
/// Deliberately has no `Default`: a storage layer must never pick a metric on
/// the query's behalf, and every similarity computation names its metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SimilarityMetric {
    /// Cosine similarity in `[-1, 1]`; higher means closer. Undefined for a
    /// zero-norm operand.
    Cosine,
    /// Inner product; higher means closer. Unbounded.
    DotProduct,
    /// Euclidean (L2) distance; lower means closer.
    Euclidean,
}

/// Whether a similarity measurement satisfies a query threshold.
///
/// Score metrics (`Cosine` / `DotProduct`) pass when `value >= threshold`;
/// `Euclidean` is a distance and passes when `value <= threshold`.
pub fn similarity_meets_threshold(metric: SimilarityMetric, value: f64, threshold: f64) -> bool {
    match metric {
        SimilarityMetric::Cosine | SimilarityMetric::DotProduct => value >= threshold,
        SimilarityMetric::Euclidean => value <= threshold,
    }
}

/// Why a vector similarity computation was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSimilarityError {
    /// The operands declare different dimensions.
    DimensionMismatch,
    /// Cosine similarity is undefined for a zero-norm operand.
    ZeroNorm,
}

impl std::fmt::Display for VectorSimilarityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DimensionMismatch => write!(f, "vector similarity requires equal dimensions"),
            Self::ZeroNorm => write!(f, "cosine similarity is undefined for a zero-norm vector"),
        }
    }
}

impl std::error::Error for VectorSimilarityError {}

/// Wire shape of [`VectorValue`]; deserialized only to feed [`VectorValue::new`].
#[derive(Deserialize)]
struct VectorValueFields {
    dimension: u32,
    components: Vec<f32>,
}

impl<'de> Deserialize<'de> for VectorValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = VectorValueFields::deserialize(deserializer)?;
        Self::new(fields.dimension, fields.components).ok_or_else(|| {
            serde::de::Error::custom("`vector` dimension must be non-empty, match the component count and every component must be finite")
        })
    }
}

// `f32` has no reflexive `Eq`/`Ord`, so equality and the total ordering both
// compare IEEE-754 bit patterns. `VectorValue::new` rejects non-finite
// components, which keeps bit equality in agreement with `==` for every value
// this type can hold.
impl PartialEq for VectorValue {
    fn eq(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.components.len() == other.components.len()
            && self.components.iter().zip(&other.components).all(|(a, b)| a.to_bits() == b.to_bits())
    }
}

impl Eq for VectorValue {}

impl PartialOrd for VectorValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VectorValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dimension
            .cmp(&other.dimension)
            .then_with(|| self.components.iter().map(|c| c.to_bits()).cmp(other.components.iter().map(|c| c.to_bits())))
    }
}

/// Whether a text is exactly 64 lowercase hex digits (a 32-byte digest).
fn is_lower_hex64(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Reference to the immutable manifest object that lists a file's chunks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FileManifestRef {
    /// Lowercase hex digest of the manifest payload (64 hex digits).
    pub hash: String,
    /// Manifest payload size in bytes.
    pub size: u64,
}

/// Row value of the VOS `file` scalar: content identity, metadata and manifest.
///
/// Mirrors the YYDB `file` descriptor: identity is the SHA-256 digest over the
/// complete octets, while `name` / `media_type` are labelling metadata that
/// never take part in the identity.
///
/// 两个方向的 serde 都经由 [`FileDescriptorFields`] 线形态 DTO：`Deserialize`
/// 是手写的，JSON 输入无法构造出 [`FileDescriptor::new`] 会拒绝的描述符；
/// `Serialize` 同样委托给该 DTO，camelCase 线名只存在于 DTO，不在核心类型上。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileDescriptor {
    /// Lowercase hex SHA-256 over the complete file octets (64 hex digits).
    pub digest: String,
    /// Total octet count of the complete file.
    pub octets: u64,
    /// Original file name; never part of the identity.
    pub name: Option<String>,
    /// Declared media type; never part of the identity.
    pub media_type: Option<String>,
    /// Immutable chunk manifest reference.
    pub manifest: FileManifestRef,
}

impl FileDescriptor {
    /// Builds a descriptor, rejecting digests that are not 64 lowercase hex
    /// digits and metadata labels that are blank (the YYDB `file` contract
    /// accepts a label only when it carries a non-blank value).
    pub fn new(digest: String, octets: u64, name: Option<String>, media_type: Option<String>, manifest: FileManifestRef) -> Option<Self> {
        if !is_lower_hex64(&digest) || !is_lower_hex64(&manifest.hash) {
            return None;
        }
        let clean = |label: Option<String>| match &label {
            Some(text) if text.trim().is_empty() => None,
            _ => Some(label),
        };
        let name = clean(name)?;
        let media_type = clean(media_type)?;
        Some(Self { digest, octets, name, media_type, manifest })
    }
}

/// Wire shape of [`FileDescriptor`]; deserialized only to feed
/// [`FileDescriptor::new`], serialized only to emit the canonical
/// YYDB `file` tagged-scalar field names.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileDescriptorFields {
    digest: String,
    octets: u64,
    name: Option<String>,
    media_type: Option<String>,
    manifest: FileManifestRef,
}

impl Serialize for FileDescriptor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        FileDescriptorFields {
            digest: self.digest.clone(),
            octets: self.octets,
            name: self.name.clone(),
            media_type: self.media_type.clone(),
            manifest: self.manifest.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FileDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = FileDescriptorFields::deserialize(deserializer)?;
        Self::new(fields.digest, fields.octets, fields.name, fields.media_type, fields.manifest).ok_or_else(|| {
            serde::de::Error::custom("`file` digest / manifest hash must be 64 lowercase hex digits and metadata labels must be non-blank")
        })
    }
}

impl Value {
    /// Display / compare encoding used in predicates.
    pub fn as_pred_literal(&self) -> (String, database_query::ir::LiteralKind) {
        match self {
            Self::Null => ("null".into(), database_query::ir::LiteralKind::Null),
            Self::Bool(b) => (b.to_string(), database_query::ir::LiteralKind::Bool),
            Self::Int(i) => (i.to_string(), database_query::ir::LiteralKind::Int),
            Self::UInt(u) => (u.to_string(), database_query::ir::LiteralKind::UInt),
            Self::Float(f) => {
                // The VOS `f64` value model only carries finite numbers; a
                // non-finite float has no canonical literal text.
                debug_assert!(f.is_finite(), "VOS `f64` values must be finite");
                (float_to_text(*f), database_query::ir::LiteralKind::Float)
            }
            Self::Str(s) => (s.clone(), database_query::ir::LiteralKind::Str),
            Self::Decimal(s) => (s.clone(), database_query::ir::LiteralKind::Decimal),
            Self::Date(s) => (s.clone(), database_query::ir::LiteralKind::Date),
            Self::Time(s) => (s.clone(), database_query::ir::LiteralKind::Time),
            Self::DateTime(s) => (s.clone(), database_query::ir::LiteralKind::DateTime),
            Self::Bytes(octets) => (bytes_to_hex(octets), database_query::ir::LiteralKind::Bytes),
            // `vector` / `file` payloads have no VOS source literal; their
            // predicate text is the canonical YYDB tagged JSON, which only a
            // typed-binding backend round-trips.
            Self::Vector(vector) => (tagged_vector_json(vector).to_string(), database_query::ir::LiteralKind::Vector),
            Self::File(file) => (tagged_file_json(file).to_string(), database_query::ir::LiteralKind::File),
            Self::Object(_) => ("null".into(), database_query::ir::LiteralKind::Null),
        }
    }
}

/// Round-trip-exact decimal text of a finite `f64`.
///
/// Rust's `Display` implementation for `f64` prints the shortest decimal that
/// parses back to the same bit pattern, so a literal produced here never
/// loses precision when a backend binds or compares it.
pub fn float_to_text(value: f64) -> String {
    value.to_string()
}

/// Encodes octets as the lowercase hex text of the VOS `bytes` wire form.
///
/// The encoding comes from VOS itself, so a literal produced here always
/// decodes through [`hex_to_bytes`] without an intermediate text guess.
pub fn bytes_to_hex(octets: &[u8]) -> String {
    vos::types::BytesValue::new(octets.to_vec()).to_wire().data
}

/// Decodes lowercase VOS hex text back to octets.
///
/// Text outside the canonical wire form yields `None` instead of a guess;
/// every literal produced by [`Value::as_pred_literal`] is canonical.
pub fn hex_to_bytes(text: &str) -> Option<Vec<u8>> {
    let wire = vos::types::BytesValueWire {
        format_version: vos::types::BYTES_VALUE_FORMAT_VERSION.to_owned(),
        encoding: "hex".to_owned(),
        data: text.to_owned(),
    };
    vos::types::BytesValue::from_wire(wire).ok().map(vos::types::BytesValue::into_octets)
}

/// JSON value of a tagged `bytes` carrier: `{"$database":"bytes","hex":"<lowercase hex>"}`.
///
/// This is the one `bytes` shape the N-API host, the browser WASM host and
/// YYDB's tagged value wire all speak, so octets never degrade into text.
pub fn tagged_bytes_json(octets: &[u8]) -> JsonValue {
    let mut object = serde_json::Map::new();
    object.insert(TAGGED_JSON_KEY.to_owned(), JsonValue::String("bytes".to_owned()));
    object.insert("hex".to_owned(), JsonValue::String(bytes_to_hex(octets)));
    JsonValue::Object(object)
}

/// Octets of a tagged `bytes` carrier, or `None` for any other JSON shape.
///
/// Malformed hex is `None` as well: callers that already know the value claims
/// to be `bytes` should report the failure instead of guessing.
pub fn bytes_from_tagged_json(value: &JsonValue) -> Option<Vec<u8>> {
    let object = value.as_object()?;
    if object.get(TAGGED_JSON_KEY)?.as_str()? != "bytes" {
        return None;
    }
    hex_to_bytes(object.get("hex")?.as_str()?)
}

/// Tag name of a tagged carrier (`"bytes"`, `"vector"` or `"file"`), or `None`
/// for any JSON shape that is not one of ours.
///
/// The claim is reported even when the payload is malformed, so hosts can
/// refuse a broken carrier instead of guessing a text value.
pub fn tagged_json_kind(value: &JsonValue) -> Option<&str> {
    value.as_object()?.get(TAGGED_JSON_KEY)?.as_str()
}

/// JSON value of a tagged `vector` carrier in the canonical YYDB shape:
/// `{"$database":"vector","dimension":<u32>,"components":"<lowercase little-endian f32 hex>"}`.
///
/// The hex payload is the canonical `f32` binary form, so components cross the
/// JSON boundary without ever riding through a text guess.
pub fn tagged_vector_json(vector: &VectorValue) -> JsonValue {
    let mut object = serde_json::Map::new();
    object.insert(TAGGED_JSON_KEY.to_owned(), JsonValue::String("vector".to_owned()));
    object.insert("dimension".to_owned(), JsonValue::from(vector.dimension));
    object.insert("components".to_owned(), JsonValue::String(bytes_to_hex(&vector.to_le_bytes())));
    JsonValue::Object(object)
}

/// Vector of a tagged `vector` carrier, or `None` for any other JSON shape or a
/// payload whose member set, dimension, hex, length or finiteness does not
/// match the canonical YYDB form.
pub fn vector_from_tagged_json(value: &JsonValue) -> Option<VectorValue> {
    let object = value.as_object()?;
    if object.get(TAGGED_JSON_KEY)?.as_str()? != "vector" {
        return None;
    }
    if object.len() != 3 || !object.contains_key("dimension") || !object.contains_key("components") {
        return None;
    }
    let dimension = u32::try_from(object.get("dimension")?.as_u64()?).ok().filter(|dimension| *dimension > 0)?;
    let octets = hex_to_bytes(object.get("components")?.as_str()?)?;
    VectorValue::from_le_bytes(dimension, &octets)
}

/// JSON value of a tagged `file` carrier in the canonical YYDB shape:
/// `{"$database":"file","digest":"<64 lowercase hex>","octets":<u64>[,"name":…][,"mediaType":…],"manifest":{"hash":"<64 lowercase hex>","size":<u64>}}`.
///
/// Optional metadata members are omitted entirely when absent, matching the
/// encoding YYDB itself writes on its tagged value wire.
pub fn tagged_file_json(file: &FileDescriptor) -> JsonValue {
    let mut object = serde_json::Map::new();
    object.insert(TAGGED_JSON_KEY.to_owned(), JsonValue::String("file".to_owned()));
    object.insert("digest".to_owned(), JsonValue::String(file.digest.clone()));
    object.insert("octets".to_owned(), JsonValue::from(file.octets));
    if let Some(name) = &file.name {
        object.insert("name".to_owned(), JsonValue::String(name.clone()));
    }
    if let Some(media_type) = &file.media_type {
        object.insert("mediaType".to_owned(), JsonValue::String(media_type.clone()));
    }
    object.insert("manifest".to_owned(), json!({ "hash": file.manifest.hash, "size": file.manifest.size }));
    JsonValue::Object(object)
}

/// File descriptor of a tagged `file` carrier, or `None` for any other JSON
/// shape or a payload that does not match the canonical YYDB file form: the
/// member set, the digest spellings, the label values and the manifest shape
/// are all part of the stored value, so a truncated or padded descriptor is
/// rejected instead of decoding into a file that claims another identity.
pub fn file_from_tagged_json(value: &JsonValue) -> Option<FileDescriptor> {
    let object = value.as_object()?;
    if object.get(TAGGED_JSON_KEY)?.as_str()? != "file" {
        return None;
    }
    const ALLOWED: [&str; 6] = [TAGGED_JSON_KEY, "digest", "octets", "name", "mediaType", "manifest"];
    if object.keys().any(|member| !ALLOWED.contains(&member.as_str())) {
        return None;
    }
    let digest = object.get("digest")?.as_str()?.to_owned();
    let octets = object.get("octets")?.as_u64()?;
    let label = |member: &str| -> Option<Option<String>> {
        match object.get(member) {
            None | Some(JsonValue::Null) => Some(None),
            Some(JsonValue::String(text)) if !text.trim().is_empty() => Some(Some(text.to_owned())),
            Some(_) => None,
        }
    };
    let name = label("name")?;
    let media_type = label("mediaType")?;
    let manifest = object.get("manifest")?.as_object()?;
    if manifest.len() != 2 || !manifest.contains_key("hash") || !manifest.contains_key("size") {
        return None;
    }
    let hash = manifest.get("hash")?.as_str()?.to_owned();
    let size = manifest.get("size")?.as_u64()?;
    FileDescriptor::new(digest, octets, name, media_type, FileManifestRef { hash, size })
}

/// One logical row: ordered field map.
pub type Row = BTreeMap<String, Value>;
