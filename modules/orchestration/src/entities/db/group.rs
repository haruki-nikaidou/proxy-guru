//! Groups: how the dashboard draws the graph.
//!
//! A group gathers pods, edges, exits and servers under a kind the dashboard
//! defines (a splitter, an aggregator, a rule), with whatever the dashboard
//! needs to remember about it in `props`. Nothing is derived from a group; a
//! member that goes away leaves its groups on its own.

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::edge::EdgeId;
use crate::entities::db::exit::ExitId;
use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;
use base::db::Error;
use db_types::table_record;
use serde_json::Value;
use sqlx::PgConnection;
use sqlx::types::Json;
use std::collections::HashMap;

table_record!(GroupId, "orchestration_group");

#[derive(Debug, Clone, PartialEq)]
pub struct GroupEntity {
    pub id: GroupId,
    pub canvas: CanvasId,
    pub kind: String,
    pub name: String,
    pub props: Value,
    /// In the order they were given.
    pub members: Vec<GroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupMember {
    Pod(PodId),
    Edge(EdgeId),
    Exit(ExitId),
    Server(ServerId),
}

struct GroupRow {
    id: GroupId,
    canvas: CanvasId,
    kind: String,
    name: String,
    props: Json<Value>,
}

struct MemberRow {
    group_id: GroupId,
    pod: Option<PodId>,
    edge: Option<EdgeId>,
    exit: Option<ExitId>,
    server: Option<ServerId>,
}

/// The groups of the given canvases with their members, by id.
pub(crate) async fn groups_of_canvases(
    conn: &mut PgConnection,
    canvases: &[CanvasId],
) -> Result<Vec<GroupEntity>, Error> {
    let rows = sqlx::query_as!(
        GroupRow,
        r#"SELECT id AS "id: GroupId", canvas AS "canvas: CanvasId", kind, name,
                  props AS "props: Json<Value>"
           FROM orchestration_group WHERE canvas = ANY($1) ORDER BY id"#,
        canvases as _
    )
    .fetch_all(&mut *conn)
    .await?;
    let members = sqlx::query_file_as!(
        MemberRow,
        "sql/groups_of_canvases_members.sql",
        canvases as _
    )
    .fetch_all(conn)
    .await?;
    let mut by_group: HashMap<GroupId, Vec<GroupMember>> = HashMap::new();
    for row in members {
        let member = match (row.pod, row.edge, row.exit, row.server) {
            (Some(pod), None, None, None) => GroupMember::Pod(pod),
            (None, Some(edge), None, None) => GroupMember::Edge(edge),
            (None, None, Some(exit), None) => GroupMember::Exit(exit),
            (None, None, None, Some(server)) => GroupMember::Server(server),
            _ => continue,
        };
        by_group.entry(row.group_id).or_default().push(member);
    }
    Ok(rows
        .into_iter()
        .map(|row| GroupEntity {
            members: by_group.remove(&row.id).unwrap_or_default(),
            id: row.id,
            canvas: row.canvas,
            kind: row.kind,
            name: row.name,
            props: row.props.0,
        })
        .collect())
}

pub(crate) async fn insert_group(
    conn: &mut PgConnection,
    group: &GroupEntity,
) -> Result<(), Error> {
    sqlx::query!(
        "INSERT INTO orchestration_group (id, canvas, kind, name, props)
         VALUES ($1, $2, $3, $4, $5)",
        group.id as _,
        group.canvas as _,
        group.kind,
        group.name,
        Json(&group.props) as _
    )
    .execute(&mut *conn)
    .await?;
    insert_members(conn, group).await
}

/// Rewrites an existing group, members included.
pub(crate) async fn update_group(
    conn: &mut PgConnection,
    group: &GroupEntity,
) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE orchestration_group SET kind = $2, name = $3, props = $4 WHERE id = $1",
        group.id as _,
        group.kind,
        group.name,
        Json(&group.props) as _
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "DELETE FROM orchestration_group_member WHERE group_id = $1",
        group.id as _
    )
    .execute(&mut *conn)
    .await?;
    insert_members(conn, group).await
}

async fn insert_members(conn: &mut PgConnection, group: &GroupEntity) -> Result<(), Error> {
    for (position, member) in group.members.iter().enumerate() {
        let (pod, edge, exit, server) = match member {
            GroupMember::Pod(id) => (Some(id), None, None, None),
            GroupMember::Edge(id) => (None, Some(id), None, None),
            GroupMember::Exit(id) => (None, None, Some(id), None),
            GroupMember::Server(id) => (None, None, None, Some(id)),
        };
        sqlx::query!(
            "INSERT INTO orchestration_group_member (group_id, position, pod, edge, exit, server)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT DO NOTHING",
            group.id as _,
            i32::try_from(position).unwrap_or(i32::MAX),
            pod as _,
            edge as _,
            exit as _,
            server as _
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
