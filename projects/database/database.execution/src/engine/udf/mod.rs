//! 嵌入式宿主上的 UDF 面。
//!
//! - **简单路径：** [`ScalarUdf`] + [`crate::engine::Connection::create_scalar`]。
//! - **进阶路径：** 从 `database-udf` 再导出的 [`UdfRegistry`] 等。
//!
//! 其他语言应通过 `database-client` 走服务端协议，而不是嵌入本 crate。

mod api;
mod session;

pub use api::*;
pub(crate) use session::UdfSubsystem;
