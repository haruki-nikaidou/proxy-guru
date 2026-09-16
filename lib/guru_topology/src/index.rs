//! Lookups over a [`Graph`], built once per call.

use crate::diagnostic::{Diagnostic, Problem, Subject};
use crate::model::{Edge, EdgeId, EdgeTarget, Exit, ExitId, Graph, Pod, PodId, Server, ServerId};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct Index<'g> {
    pub servers: BTreeMap<&'g ServerId, &'g Server>,
    pub pods: BTreeMap<&'g PodId, &'g Pod>,
    pub exits: BTreeMap<&'g ExitId, &'g Exit>,
    pub edges: BTreeMap<&'g EdgeId, &'g Edge>,
    /// Each known pod's out-edges, by edge id.
    out: BTreeMap<&'g PodId, Vec<&'g Edge>>,
    /// The edges into each known pod, by edge id.
    into_pod: BTreeMap<&'g PodId, Vec<&'g Edge>>,
    /// The edges into each known exit, by edge id.
    into_exit: BTreeMap<&'g ExitId, Vec<&'g Edge>>,
}

impl<'g> Index<'g> {
    /// Indexes the graph. The first of several items sharing an id is the one
    /// indexed; each shared id is reported once.
    pub fn build(graph: &'g Graph, out: &mut Vec<Diagnostic>) -> Self {
        let servers = unique(&graph.servers, |s| &s.id, Subject::Server, out);
        let pods = unique(&graph.pods, |p| &p.id, Subject::Pod, out);
        let exits = unique(&graph.exits, |e| &e.id, Subject::Exit, out);
        let edges = unique(&graph.edges, |e| &e.id, Subject::Edge, out);

        let mut out_edges: BTreeMap<&PodId, Vec<&Edge>> = BTreeMap::new();
        let mut into_pod: BTreeMap<&PodId, Vec<&Edge>> = BTreeMap::new();
        let mut into_exit: BTreeMap<&ExitId, Vec<&Edge>> = BTreeMap::new();
        for edge in edges.values().copied() {
            if let Some((id, _)) = pods.get_key_value(&edge.source) {
                out_edges.entry(*id).or_default().push(edge);
            }
            match &edge.target {
                EdgeTarget::Pod(target) => {
                    if let Some((id, _)) = pods.get_key_value(target) {
                        into_pod.entry(*id).or_default().push(edge);
                    }
                }
                EdgeTarget::Exit(target) => {
                    if let Some((id, _)) = exits.get_key_value(target) {
                        into_exit.entry(*id).or_default().push(edge);
                    }
                }
            }
        }

        Self {
            servers,
            pods,
            exits,
            edges,
            out: out_edges,
            into_pod,
            into_exit,
        }
    }

    pub fn out_of(&self, pod: &PodId) -> &[&'g Edge] {
        self.out.get(pod).map(Vec::as_slice).unwrap_or_default()
    }

    pub fn edges_into_pod(&self, pod: &PodId) -> &[&'g Edge] {
        self.into_pod
            .get(pod)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn edges_into_exit(&self, exit: &ExitId) -> &[&'g Edge] {
        self.into_exit
            .get(exit)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// A pod's name for messages, or its id when the graph does not have it.
    pub fn pod_name(&self, pod: &PodId) -> String {
        self.pods
            .get(pod)
            .map_or_else(|| pod.to_string(), |p| p.name.clone())
    }

    pub fn server_name(&self, server: &ServerId) -> String {
        self.servers
            .get(server)
            .map_or_else(|| server.to_string(), |s| s.name.clone())
    }
}

fn unique<'g, T, K: Ord + Clone + 'g>(
    items: &'g [T],
    id: impl Fn(&'g T) -> &'g K,
    subject: impl Fn(K) -> Subject,
    out: &mut Vec<Diagnostic>,
) -> BTreeMap<&'g K, &'g T> {
    let mut map: BTreeMap<&K, &T> = BTreeMap::new();
    let mut shared: BTreeSet<&K> = BTreeSet::new();
    for item in items {
        let key = id(item);
        if map.contains_key(key) {
            shared.insert(key);
        } else {
            map.insert(key, item);
        }
    }
    for key in shared {
        let subject = subject(key.clone());
        let message = format!("{} is used by more than one item", describe(&subject));
        out.push(Diagnostic::new(
            Problem::DuplicateId,
            vec![subject],
            message,
        ));
    }
    map
}

fn describe(subject: &Subject) -> String {
    match subject {
        Subject::Server(id) => format!("server id {id}"),
        Subject::Pod(id) => format!("pod id {id}"),
        Subject::Exit(id) => format!("exit id {id}"),
        Subject::Edge(id) => format!("edge id {id}"),
    }
}
