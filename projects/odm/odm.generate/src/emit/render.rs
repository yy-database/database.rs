//! Dejavu rendering entry points for generated artifacts.

use super::*;

#[allow(dead_code)]
pub(crate) fn render_aot_ir(ir_json: &'static str, ctx: &Value) -> Result<String> {
    static CACHE: OnceCell<Mutex<HashMap<usize, IrDocument>>> = OnceCell::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = ir_json.as_ptr() as usize;
    let doc = {
        let mut guard = cache.lock().expect("ir cache lock");
        if let Some(doc) = guard.get(&key) {
            doc.clone()
        }
        else {
            let doc: IrDocument = serde_json::from_str(ir_json).map_err(|e| Error::InvalidIr("embedded".into(), e.to_string()))?;
            guard.insert(key, doc.clone());
            doc
        }
    };
    Dejavu::render(&doc, ctx).map_err(|e| Error::Dejavu(e.to_string()))
}

#[allow(dead_code)]
pub(crate) fn render_dyn_source(source: &str, ctx: &Value) -> Result<String> {
    Dejavu::render_source(source, ctx).map_err(|e| Error::Dejavu(format!("{e:?}")))
}

/// Marker kept for older Phase 0 callers.
pub fn is_stub() -> bool {
    false
}

/// Convert a VOS `OperationKind` enum into the stable lowercase string tag
/// we store inside `OperationModel.kind`.
pub(crate) fn operation_kind_label(kind: vos::types::OperationKind) -> String {
    match kind {
        vos::types::OperationKind::Query => "query",
        vos::types::OperationKind::Udf => "udf",
        vos::types::OperationKind::Micro => "micro",
        vos::types::OperationKind::Macro => "macro",
    }
    .to_owned()
}

// -----------------------------------------------------------------------------
// Operation stub emitters (Rust + TypeScript)
// -----------------------------------------------------------------------------

/// Emit Rust stub function declarations for every projected operation.
/// Returns an empty string when no operations are present.
///
/// Signature types come from the declared-vos carrier resolver only; an
/// operation the schema cannot carry is a hard failure, never a `String` stub.
pub(crate) fn emit_rust_operation_stubs(model: &GenerationModel) -> Result<String> {
    if model.operations.is_empty() {
        return Ok(String::new());
    }
    let declared: HashSet<String> = model.tables.iter().map(|table| table.name.clone()).collect();
    let mut out = String::new();
    out.push_str("// --- Projected operation stubs (from VOS OperationProjectionArtifact) ---\n");
    out.push_str("// These are thin declarations. Implement bodies in your host adapter.\n\n");
    for op in &model.operations {
        let ret = match op.return_type.as_ref() {
            Some(ret) => resolve_operation_carrier(&op.name, ret, &declared)?.rust_ty,
            None => "()".to_owned(),
        };
        let kind_doc = operation_kind_rust_doc(op.kind.as_str())
            .ok_or_else(|| Error::UnsupportedOperationKind { operation: op.name.clone(), kind: op.kind.clone() })?;
        out.push_str(&format!(
            "/// VOS {kind_doc:?} operation `{name}` (op_id: {op_id}).\n\
             fn {name}({params}) -> {ret} {{ unimplemented!(\"VOS op {name}\") }}\n\n",
            kind_doc = kind_doc,
            name = op.name,
            op_id = op.operation_id,
            params = emit_rust_params(op, &declared)?,
            ret = ret,
        ));
    }
    Ok(out)
}

fn emit_rust_params(op: &OperationModel, declared: &HashSet<String>) -> Result<String> {
    op.params
        .iter()
        .map(|p| Ok(format!("{}: {}", p.name, resolve_operation_carrier(&op.name, &p.vos_type, declared)?.rust_ty)))
        .collect::<Result<Vec<_>>>()
        .map(|params| params.join(", "))
}

pub(crate) fn operation_kind_rust_doc(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "query" => "query",
        "udf" => "udf",
        "micro" => "micro",
        "macro" => "macro",
        _ => return None,
    })
}

/// Emit TypeScript stub method declarations for every projected operation.
/// Returns an empty string when no operations are present.
pub(crate) fn emit_ts_operation_stubs(model: &GenerationModel) -> Result<String> {
    if model.operations.is_empty() {
        return Ok(String::new());
    }
    let declared: HashSet<String> = model.tables.iter().map(|table| table.name.clone()).collect();
    let mut out = String::new();
    out.push_str("\n/** --- Projected operation stubs (from VOS OperationProjectionArtifact) ---\n");
    out.push_str(" * These delegate to `this.executor` with the declared operation id.\n");
    out.push_str(" */\n");
    out.push_str("export class GeneratedOperations {\n");
    out.push_str("    private readonly executor: OperationExecutor;\n\n");
    out.push_str("    constructor(executor: OperationExecutor) {\n");
    out.push_str("        this.executor = executor;\n");
    out.push_str("    }\n\n");
    for op in &model.operations {
        let ts_ret = match op.return_type.as_ref() {
            Some(t) => resolve_operation_carrier(&op.name, t, &declared)?.ts_type,
            None => "void".to_owned(),
        };
        let param_sig = op.params.iter().map(|p| format!("{}: {}", p.name, p.ts_type)).collect::<Vec<_>>().join(", ");
        let param_values = op.params.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", ");
        // Only declared `&T` reference parameters may flatten `{ pk }` /
        // Reference write inputs; every other carrier passes through untouched.
        let ref_flags = op.params.iter().map(|p| p.vos_type.starts_with('&').to_string()).collect::<Vec<_>>().join(", ");
        let use_execute = ts_ret == "void";
        let kind_label = operation_kind_ts_label(op.kind.as_str())
            .ok_or_else(|| Error::UnsupportedOperationKind { operation: op.name.clone(), kind: op.kind.clone() })?;
        out.push_str(&format!(
            "    /** VOS {kind} operation `{name}` (op_id: {op_id}). */\n\
             async {name}({param_sig}): Promise<{ts_ret}> {{\n\
             {indent}const synthesis = synthesizeOperationCall(\"{name}\", [{param_values}], [{ref_flags}]);\n\
             {indent}const envelope = await this.executor.{exec_fn}(buildDeclaredOperationRequest(\"{op_id}\", synthesis));\n",
            kind = kind_label,
            name = op.name,
            op_id = op.operation_id,
            param_sig = param_sig,
            param_values = param_values,
            ts_ret = ts_ret,
            indent = "        ",
            exec_fn = if use_execute { "executeUnit" } else { "execute" },
        ));
        out.push_str("        if (!envelope.ok) {\n");
        out.push_str("            throwOperationEnvelopeError(envelope);\n");
        out.push_str("        }\n");
        if !use_execute {
            out.push_str("        return envelope.value as any;\n");
        }
        out.push_str("    }\n\n");
    }
    out.push_str("}\n");
    Ok(out)
}

fn operation_kind_ts_label(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "query" => "query",
        "udf" => "udf",
        "micro" => "micro",
        "macro" => "macro",
        _ => return None,
    })
}
