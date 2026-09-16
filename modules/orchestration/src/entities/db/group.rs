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

#[derive(sqlx::FromRow)]
struct GroupRow {
    id: GroupId,
    canvas: CanvasId,
    kind: String,
    name: String,
    props: Json<Value>,
}

#[derive(sqlx::FromRow)]
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
    let rows: Vec<GroupRow> =
        sqlx::query_as("SELECT * FROM orchestration_group WHERE canvas = ANY($1) ORDER BY id")
            .bind(canvases)
            .fetch_all(&mut *conn)
            .await?;
    let members: Vec<MemberRow> = sqlx::query_as(
        "SELECT m.group_id, m.pod, m.edge, m.exit, m.server
         FROM orchestration_group_member m
         JOIN orchestration_group g ON g.id = m.group_id
         WHERE g.canvas = ANY($1)
         ORDER BY m.group_id, m.position",
    )
    .bind(canvases)
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
    sqlx::query(
        "INSERT INTO orchestration_group (id, canvas, kind, name, props) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&group.id)
    .bind(&group.canvas)
    .bind(&group.kind)
    .bind(&group.name)
    .bind(Json(&group.props))
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
        sqlx::query(
            "INSERT INTO orchestration_group_member (group_id, position, pod, edge, exit, server)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT DO NOTHING",
        )
        .bind(&group.id)
        .bind(i32::try_from(position).unwrap_or(i32::MAX))
        .bind(pod)
        .bind(edge)
        .bind(exit)
        .bind(server)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
