//! 跨作者面共享的对象—数据库映射模型。
//!
//! ODM（Object-Database Mapping）涵盖 ORM、文档向、OGM、ONDM 等；
//! 详见 `projects/odm/readme.md`。
//!
//! `odm.input.*` 把 GraphQL / SQL / VOS 等 DDL 降到本模型，适配层再落到具体存储。

use serde::{Deserialize, Serialize};

/// 统一映射模型：一组实体与其间关系。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MappingModel {
    /// 模型逻辑名；空串表示匿名输入。
    pub name: String,
    /// 实体声明，顺序稳定以便指纹与生成。
    pub entities: Vec<EntityDecl>,
    /// 实体间关系；可为空。
    pub relations: Vec<RelationDecl>,
}

/// 一个可持久化的对象类型（表 / 集合 / 节点标签等）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityDecl {
    /// 稳定实体名（不含命名空间前缀时由输入面自行规范化）。
    pub name: String,
    /// 可选命名空间或 schema 前缀。
    pub namespace: Option<String>,
    /// 字段列表。
    pub fields: Vec<FieldDecl>,
    /// 目标形态提示；不影响合同合法性，只指导适配。
    pub kind_hint: EntityKindHint,
}

/// 实体在存储侧的形态提示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EntityKindHint {
    /// 未指定；适配层按数据源默认处理。
    #[default]
    Unspecified,
    /// 关系表行。
    Relational,
    /// 文档集合。
    Document,
    /// 图节点。
    GraphNode,
    /// 键值条目族。
    KeyValue,
}

/// 实体上的一个字段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDecl {
    /// 字段名。
    pub name: String,
    /// 逻辑类型名（如 `string`、`i64`、`uuid`）；不绑具体引擎 OID。
    pub type_name: String,
    /// 是否允许空。
    pub nullable: bool,
    /// 是否为主键组成部分。
    pub primary_key: bool,
    /// 是否要求唯一。
    pub unique: bool,
}

/// 实体间关系。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationDecl {
    /// 关系名。
    pub name: String,
    /// 起点实体。
    pub from_entity: String,
    /// 终点实体。
    pub to_entity: String,
    /// 基数：`1:1` / `1:n` / `n:n` 等自由文本，输入面负责规范化。
    pub cardinality: String,
}

impl MappingModel {
    /// 构造空模型。
    pub fn empty() -> Self {
        Self::default()
    }

    /// 按实体名查找；大小写敏感。
    pub fn entity(&self, name: &str) -> Option<&EntityDecl> {
        self.entities.iter().find(|entity| entity.name == name)
    }
}
