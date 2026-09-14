use crate::entities::surreal::connection::EdgeConnectionEntity;
use crate::entities::surreal::node::NodeWithPorts;
use crate::entities::surreal::server::ServerEntity;
use crate::utils::ids::record_key;
use kanau::processor::Processor;
use newtype_record_id::table_record;
use std::collections::HashMap;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(CanvasId, "orchestration_canvas");

#[derive(Debug, Clone, SurrealValue)]
pub struct CanvasEntity {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
    /// Bumped by every mutating transaction; the derivation fence.
    pub generation: i64,
    /// The generation the stored config views were derived from.
    pub derived_generation: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
pub struct CanvasUiPosition {
    pub x: i64,
    pub y: i64,
}

/// Everything the dashboard renders for one canvas.
#[derive(Debug, Clone)]
pub struct CanvasContents {
    pub canvas: CanvasEntity,
    /// Root first, parent last; empty for a root.
    pub ancestors: Vec<CanvasEntity>,
    /// The canvases this canvas's import nodes embed.
    pub import_targets: Vec<CanvasEntity>,
    pub servers: Vec<ServerEntity>,
    pub nodes: Vec<NodeWithPorts>,
    pub edges: Vec<EdgeConnectionEntity>,
}

/// A canvas tree, children ordered by record key.
#[derive(Debug, Clone)]
pub struct CanvasTree {
    pub canvas: CanvasEntity,
    pub children: Vec<CanvasTree>,
}

#[derive(Debug)]
pub struct CreateCanvas {
    pub name: String,
    pub description: String,
}

impl Processor<CreateCanvas> for SurrealProcessor {
    type Output = CanvasEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:CreateCanvas", skip_all, err)]
    async fn process(&self, input: CreateCanvas) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "CREATE ONLY orchestration_canvas CONTENT {
                     name: $name, description: $description,
                     generation: 0, derived_generation: 0
                 }",
            )
            .bind(("name", input.name))
            .bind(("description", input.description))
            .await?;
        resp.take::<Option<CanvasEntity>>(0)?
            .ok_or_else(|| surrealdb::Error::internal("create canvas returned no row".to_string()))
    }
}

/// Lists canvases; `include_subcanvases: false` lists only roots.
#[derive(Debug)]
pub struct ListCanvases {
    pub include_subcanvases: bool,
}

impl Processor<ListCanvases> for SurrealProcessor {
    type Output = Vec<CanvasEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCanvases", skip_all, err, fields(result_count))]
    async fn process(&self, input: ListCanvases) -> Result<Self::Output, Self::Error> {
        let sql = if input.include_subcanvases {
            "SELECT * FROM orchestration_canvas"
        } else {
            "SELECT * FROM orchestration_canvas WHERE id NOT IN
                (SELECT VALUE spec.config.canvas FROM orchestration_node WHERE spec.config.canvas != NONE)"
        };
        let mut resp = self.db().query(sql).await?;
        let result = resp.take::<Vec<CanvasEntity>>(0)?;
        tracing::Span::current().record("result_count", result.len());
        Ok(result)
    }
}

/// The root of the tree `canvas` belongs to (`canvas` itself for a root).
#[derive(Debug)]
pub struct FindRootCanvas {
    pub canvas: CanvasId,
}

impl Processor<FindRootCanvas> for SurrealProcessor {
    type Output = Option<CanvasEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindRootCanvas", skip_all, err)]
    async fn process(&self, input: FindRootCanvas) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM ONLY fn::orchestration_root($canvas)")
            .bind(("canvas", input.canvas))
            .await?;
        resp.take::<Option<CanvasEntity>>(0)
    }
}

#[derive(Debug, Clone, SurrealValue)]
struct ImportLink {
    canvas: CanvasId,
    target: CanvasId,
}

/// The whole tree containing `canvas`, from its root. `None` when the canvas
/// does not exist.
#[derive(Debug)]
pub struct LoadCanvasTree {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasTree> for SurrealProcessor {
    type Output = Option<CanvasTree>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasTree", skip_all, err)]
    async fn process(&self, input: LoadCanvasTree) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1-2 the LETs; canvases at 3, links at 4, root at 5.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/canvas/load_canvas_tree.surql"))
            .bind(("canvas", input.canvas))
            .await?;
        let canvases = resp.take::<Vec<CanvasEntity>>(3)?;
        let links = resp.take::<Vec<ImportLink>>(4)?;
        let Some(root) = resp.take::<Option<CanvasId>>(5)? else {
            return Ok(None);
        };
        if canvases.is_empty() {
            return Ok(None);
        }
        let mut by_key: HashMap<String, CanvasEntity> = canvases
            .into_iter()
            .map(|c| (record_key(&c.id.0), c))
            .collect();
        let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
        for link in links {
            children_of
                .entry(record_key(&link.canvas.0))
                .or_default()
                .push(record_key(&link.target.0));
        }
        Ok(assemble_tree(
            &record_key(&root.0),
            &mut by_key,
            &children_of,
            0,
        ))
    }
}

fn assemble_tree(
    key: &str,
    by_key: &mut HashMap<String, CanvasEntity>,
    children_of: &HashMap<String, Vec<String>>,
    depth: usize,
) -> Option<CanvasTree> {
    // The schema functions already refuse deeper trees; the guard only keeps a
    // corrupt link set from recursing forever.
    if depth > 32 {
        return None;
    }
    let canvas = by_key.remove(key)?;
    let mut child_keys = children_of.get(key).cloned().unwrap_or_default();
    child_keys.sort();
    let children = child_keys
        .iter()
        .filter_map(|child| assemble_tree(child, by_key, children_of, depth.saturating_add(1)))
        .collect();
    Some(CanvasTree { canvas, children })
}

#[derive(Debug)]
pub struct FindCanvasById {
    pub id: CanvasId,
}

impl Processor<FindCanvasById> for SurrealProcessor {
    type Output = Option<CanvasEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindCanvasById", skip_all, err)]
    async fn process(&self, input: FindCanvasById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<CanvasEntity>>(0)
    }
}

#[derive(Debug)]
pub struct UpdateCanvasMeta {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
}

impl Processor<UpdateCanvasMeta> for SurrealProcessor {
    type Output = CanvasEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:UpdateCanvasMeta", skip_all, err, fields(canvas_id = ?input.id))]
    async fn process(&self, input: UpdateCanvasMeta) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("UPDATE $id SET name = $name, description = $description RETURN AFTER")
            .bind(("id", input.id))
            .bind(("name", input.name))
            .bind(("description", input.description))
            .await?;
        resp.take::<Option<CanvasEntity>>(0)?
            .ok_or_else(|| surrealdb::Error::internal("canvas not found".to_string()))
    }
}

/// Deletes a canvas and its whole tree; refuses an imported canvas.
#[derive(Debug)]
pub struct DeleteCanvasRow {
    pub id: CanvasId,
}

impl Processor<DeleteCanvasRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteCanvasRow", skip_all, err, fields(canvas_id = ?input.id))]
    async fn process(&self, input: DeleteCanvasRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!("../../../sql/canvas/delete_canvas_row.surql"))
            .bind(("id", input.id))
            .await?
            .check()?;
        Ok(())
    }
}
