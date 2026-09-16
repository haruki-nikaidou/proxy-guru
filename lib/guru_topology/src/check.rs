//! Whether a graph may be stored.

use crate::diagnostic::{Diagnostic, Problem, Report, Subject};
use crate::index::Index;
use crate::model::{
    Edge, EdgeTarget, ExitId, Graph, Ingress, MAX_ROUTE_DEPTH, Pod, PodId, Route, ServerId,
};
use guru_worker_config::Remote;
use std::collections::BTreeMap;
use std::net::IpAddr;

/// Every problem with the graph. Errors mean it must not be stored; warnings
/// are for the operator and never block an edit.
pub fn check(graph: &Graph) -> Report {
    let mut out = Vec::new();
    let index = Index::build(graph, &mut out);
    references(&index, &mut out);
    values(&index, &mut out);
    routes(&index, &mut out);
    client_pods_dialed(&index, &mut out);
    cycles(&index, &mut out);
    listeners(&index, &mut out);
    sticky(&index, &mut out);
    unused(&index, &mut out);
    Report::from_unsorted(out)
}

fn references(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        if !index.servers.contains_key(&pod.server) {
            out.push(Diagnostic::new(
                Problem::UnknownServer,
                vec![
                    Subject::Pod(pod.id.clone()),
                    Subject::Server(pod.server.clone()),
                ],
                format!(
                    "pod {} is on server {}, which does not exist",
                    pod.name, pod.server
                ),
            ));
        }
    }
    for edge in index.edges.values() {
        if !index.pods.contains_key(&edge.source) {
            let exit = ExitId::new(edge.source.as_str());
            if index.exits.contains_key(&exit) {
                out.push(Diagnostic::new(
                    Problem::EdgeFromExit,
                    vec![Subject::Edge(edge.id.clone()), Subject::Exit(exit)],
                    format!("edge {} starts at an exit; traffic ends at exits", edge.id),
                ));
            } else {
                out.push(Diagnostic::new(
                    Problem::UnknownEdgeSource,
                    vec![Subject::Edge(edge.id.clone())],
                    format!(
                        "edge {} starts at pod {}, which does not exist",
                        edge.id, edge.source
                    ),
                ));
            }
        }
        let missing = match &edge.target {
            EdgeTarget::Pod(pod) => (!index.pods.contains_key(pod)).then(|| format!("pod {pod}")),
            EdgeTarget::Exit(exit) => {
                (!index.exits.contains_key(exit)).then(|| format!("exit {exit}"))
            }
        };
        if let Some(missing) = missing {
            out.push(Diagnostic::new(
                Problem::UnknownEdgeTarget,
                vec![Subject::Edge(edge.id.clone())],
                format!("edge {} leads to {missing}, which does not exist", edge.id),
            ));
        }
    }
}

fn values(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        let subject = || vec![Subject::Pod(pod.id.clone())];
        if pod.port == 0 {
            out.push(Diagnostic::new(
                Problem::InvalidPort,
                subject(),
                format!("pod {} listens on port 0", pod.name),
            ));
        }
        if let Some(bind) = &pod.bind_ip
            && bind.parse::<IpAddr>().is_err()
        {
            out.push(Diagnostic::new(
                Problem::InvalidBindIp,
                subject(),
                format!(
                    "pod {} binds to {bind:?}, which is not an IP address",
                    pod.name
                ),
            ));
        }
        if let Some(advertise) = &pod.advertise_ip
            && advertise.parse::<IpAddr>().is_err()
        {
            out.push(Diagnostic::new(
                Problem::InvalidAdvertiseIp,
                subject(),
                format!(
                    "pod {} advertises {advertise:?}, which is not an IP address",
                    pod.name
                ),
            ));
        }
        if let Ingress::ClientTls { sni, .. } = &pod.ingress
            && !plain_host_name(sni)
        {
            out.push(Diagnostic::new(
                Problem::InvalidSni,
                subject(),
                format!(
                    "pod {} terminates TLS for {sni:?}; an SNI is a host name with at least two labels and no wildcard",
                    pod.name
                ),
            ));
        }
    }
    for edge in index.edges.values() {
        if edge.override_port == Some(0) {
            out.push(Diagnostic::new(
                Problem::InvalidOverridePort,
                vec![Subject::Edge(edge.id.clone())],
                format!("edge {} dials port 0", edge.id),
            ));
        }
        if let Some(host) = &edge.override_ip
            && !dialable_host(host)
        {
            out.push(Diagnostic::new(
                Problem::InvalidOverrideAddress,
                vec![Subject::Edge(edge.id.clone())],
                format!(
                    "edge {} dials {host:?}, which is neither an IP address nor a host name",
                    edge.id
                ),
            ));
        }
    }
    for exit in index.exits.values() {
        if !exit_destination(&exit.destination) {
            out.push(Diagnostic::new(
                Problem::InvalidExitDestination,
                vec![Subject::Exit(exit.id.clone())],
                format!(
                    "exit {} goes to {:?}, which is not host:port",
                    exit.name, exit.destination
                ),
            ));
        }
    }
}

fn plain_host_name(name: &str) -> bool {
    let labels: Vec<&str> = name.split('.').collect();
    labels.len() >= 2
        && labels.iter().all(|label| {
            !label.is_empty() && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
}

pub(crate) fn dialable_host(host: &str) -> bool {
    if host.parse::<IpAddr>().is_ok() {
        return true;
    }
    !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
        && Remote::parse(&format!("{host}:1")).is_ok()
}

fn exit_destination(destination: &str) -> bool {
    !destination.chars().any(char::is_whitespace) && Remote::parse(destination).is_ok()
}

fn routes(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        let out_edges = index.out_of(&pod.id);
        let Some(route) = &pod.route else {
            if !out_edges.is_empty() {
                let mut subjects = vec![Subject::Pod(pod.id.clone())];
                subjects.extend(out_edges.iter().map(|e| Subject::Edge(e.id.clone())));
                out.push(Diagnostic::new(
                    Problem::MissingRoute,
                    subjects,
                    format!("pod {} has out-edges but no route over them", pod.name),
                ));
            }
            continue;
        };
        if route.depth() > MAX_ROUTE_DEPTH {
            out.push(Diagnostic::new(
                Problem::RouteTooDeep,
                vec![Subject::Pod(pod.id.clone())],
                format!(
                    "pod {}'s route nests deeper than {MAX_ROUTE_DEPTH} levels",
                    pod.name
                ),
            ));
            continue;
        }
        route_groups(pod, route, out);

        let mut uses: BTreeMap<_, usize> = BTreeMap::new();
        for leaf in route.leaves() {
            let count = uses.entry(leaf).or_insert(0);
            *count = count.saturating_add(1);
        }
        for (leaf, count) in &uses {
            let subjects = vec![Subject::Pod(pod.id.clone()), Subject::Edge((*leaf).clone())];
            match index.edges.get(*leaf) {
                None => out.push(Diagnostic::new(
                    Problem::RouteUnknownEdge,
                    subjects.clone(),
                    format!(
                        "pod {}'s route uses edge {leaf}, which does not exist",
                        pod.name
                    ),
                )),
                Some(edge) if edge.source != pod.id => out.push(Diagnostic::new(
                    Problem::RouteForeignEdge,
                    subjects.clone(),
                    format!(
                        "pod {}'s route uses edge {leaf}, which starts at pod {}",
                        pod.name,
                        index.pod_name(&edge.source)
                    ),
                )),
                Some(_) => {}
            }
            if *count > 1 {
                out.push(Diagnostic::new(
                    Problem::RouteDuplicateEdge,
                    subjects,
                    format!("pod {}'s route uses edge {leaf} {count} times", pod.name),
                ));
            }
        }
        for edge in out_edges {
            if !uses.contains_key(&edge.id) {
                out.push(Diagnostic::new(
                    Problem::RouteMissingEdge,
                    vec![Subject::Pod(pod.id.clone()), Subject::Edge(edge.id.clone())],
                    format!("pod {}'s route leaves out its edge {}", pod.name, edge.id),
                ));
            }
        }
    }
}

fn route_groups(pod: &Pod, route: &Route, out: &mut Vec<Diagnostic>) {
    let subject = || vec![Subject::Pod(pod.id.clone())];
    let mut empty = false;
    let mut zero = false;
    let mut single = false;
    for node in route.nodes() {
        match node {
            Route::Edge(_) => {}
            Route::Balance { members, .. } => {
                empty |= members.is_empty();
                zero |= members.iter().any(|m| m.weight == 0);
            }
            Route::Failover(members) => {
                empty |= members.is_empty();
                single |= members.len() == 1;
            }
        }
    }
    if empty {
        out.push(Diagnostic::new(
            Problem::EmptyGroup,
            subject(),
            format!(
                "pod {}'s route has a balance or failover without members",
                pod.name
            ),
        ));
    }
    if zero {
        out.push(Diagnostic::new(
            Problem::ZeroWeight,
            subject(),
            format!(
                "pod {}'s route gives a balance member a weight of zero",
                pod.name
            ),
        ));
    }
    if single {
        out.push(Diagnostic::new(
            Problem::SingleTierFailover,
            subject(),
            format!(
                "pod {}'s route has a failover with a single member",
                pod.name
            ),
        ));
    }
}

fn client_pods_dialed(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        let dialed = index.edges_into_pod(&pod.id);
        if pod.ingress.is_client() && !dialed.is_empty() {
            let mut subjects = vec![Subject::Pod(pod.id.clone())];
            subjects.extend(dialed.iter().map(|e| Subject::Edge(e.id.clone())));
            out.push(Diagnostic::new(
                Problem::ClientPodDialed,
                subjects,
                format!(
                    "pod {} takes clients directly, so no edge may lead into it",
                    pod.name
                ),
            ));
        }
    }
}

/// Strongly connected components over pods (Tarjan, iterative). A component
/// of two or more pods, or one pod with an edge to itself, is a loop.
fn cycles(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    let pods: Vec<&PodId> = index.pods.keys().copied().collect();
    let position: BTreeMap<&PodId, usize> = pods.iter().enumerate().map(|(i, p)| (*p, i)).collect();
    let successors: Vec<Vec<usize>> = pods
        .iter()
        .map(|pod| {
            index
                .out_of(pod)
                .iter()
                .filter_map(|edge| match &edge.target {
                    EdgeTarget::Pod(target) => position.get(target).copied(),
                    EdgeTarget::Exit(_) => None,
                })
                .collect()
        })
        .collect();

    let count = pods.len();
    let mut order: Vec<Option<usize>> = vec![None; count];
    let mut low: Vec<usize> = vec![0; count];
    let mut on_stack = vec![false; count];
    let mut stack: Vec<usize> = Vec::new();
    let mut next_order = 0usize;
    let mut components: Vec<Vec<usize>> = Vec::new();

    for start in 0..count {
        if order.get(start).copied().flatten().is_some() {
            continue;
        }
        let mut calls: Vec<(usize, usize)> = Vec::new();
        visit(
            start,
            &mut next_order,
            &mut order,
            &mut low,
            &mut on_stack,
            &mut stack,
        );
        calls.push((start, 0));
        while let Some((node, child)) = calls.last().copied() {
            let next = successors.get(node).and_then(|s| s.get(child)).copied();
            if let Some(next) = next {
                if let Some(top) = calls.last_mut() {
                    top.1 = child.saturating_add(1);
                }
                match order.get(next).copied().flatten() {
                    None => {
                        visit(
                            next,
                            &mut next_order,
                            &mut order,
                            &mut low,
                            &mut on_stack,
                            &mut stack,
                        );
                        calls.push((next, 0));
                    }
                    Some(seen) if on_stack.get(next).copied().unwrap_or(false) => {
                        if let Some(l) = low.get_mut(node) {
                            *l = (*l).min(seen);
                        }
                    }
                    Some(_) => {}
                }
                continue;
            }
            calls.pop();
            let node_low = low.get(node).copied().unwrap_or(0);
            if let Some((parent, _)) = calls.last().copied()
                && let Some(l) = low.get_mut(parent)
            {
                *l = (*l).min(node_low);
            }
            if order.get(node).copied().flatten() == Some(node_low) {
                let mut component = Vec::new();
                while let Some(member) = stack.pop() {
                    if let Some(flag) = on_stack.get_mut(member) {
                        *flag = false;
                    }
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                components.push(component);
            }
        }
    }

    for mut component in components {
        component.sort_unstable();
        let members: Vec<&PodId> = component
            .iter()
            .filter_map(|i| pods.get(*i).copied())
            .collect();
        let edges: Vec<&Edge> = members
            .iter()
            .flat_map(|pod| index.out_of(pod).iter().copied())
            .filter(
                |edge| matches!(&edge.target, EdgeTarget::Pod(target) if members.contains(&target)),
            )
            .collect();
        if members.len() < 2 && edges.is_empty() {
            continue;
        }
        let mut subjects: Vec<Subject> =
            members.iter().map(|p| Subject::Pod((*p).clone())).collect();
        let mut edge_ids: Vec<_> = edges.iter().map(|e| e.id.clone()).collect();
        edge_ids.sort();
        subjects.extend(edge_ids.into_iter().map(Subject::Edge));
        let names: Vec<String> = members.iter().map(|p| index.pod_name(p)).collect();
        out.push(Diagnostic::new(
            Problem::Cycle,
            subjects,
            format!("traffic can loop through pods {}", names.join(", ")),
        ));
    }
}

fn visit(
    node: usize,
    next_order: &mut usize,
    order: &mut [Option<usize>],
    low: &mut [usize],
    on_stack: &mut [bool],
    stack: &mut Vec<usize>,
) {
    if let Some(slot) = order.get_mut(node) {
        *slot = Some(*next_order);
    }
    if let Some(slot) = low.get_mut(node) {
        *slot = *next_order;
    }
    if let Some(flag) = on_stack.get_mut(node) {
        *flag = true;
    }
    stack.push(node);
    *next_order = next_order.saturating_add(1);
}

/// The two transports a listener can occupy a port on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Transport {
    Tcp,
    Udp,
}

fn transport(pod: &Pod) -> Transport {
    match pod.ingress {
        Ingress::RelayQuic => Transport::Udp,
        _ => Transport::Tcp,
    }
}

/// Two pods on one server contend for a socket when they share a port and a
/// transport and their bind addresses overlap: a wildcard (or unparsable)
/// bind overlaps everything, two literals only themselves.
fn listeners(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    type Claims<'a> = BTreeMap<(&'a ServerId, u16, Transport), Vec<(Option<IpAddr>, &'a Pod)>>;
    let mut claims: Claims<'_> = BTreeMap::new();
    for pod in index.pods.values().copied() {
        let bind = pod
            .bind_ip
            .as_deref()
            .and_then(|b| b.parse::<IpAddr>().ok())
            .filter(|ip| !ip.is_unspecified());
        let claim = claims
            .entry((&pod.server, pod.port, transport(pod)))
            .or_default();
        let clash = claim.iter().find(|(other, _)| match (other, bind) {
            (None, _) | (_, None) => true,
            (Some(a), Some(b)) => *a == b,
        });
        if let Some((_, first)) = clash {
            out.push(Diagnostic::new(
                Problem::ListenerConflict,
                vec![
                    Subject::Pod(first.id.clone()),
                    Subject::Pod(pod.id.clone()),
                    Subject::Server(pod.server.clone()),
                ],
                format!(
                    "pods {} and {} both listen on port {} of server {}",
                    first.name,
                    pod.name,
                    pod.port,
                    index.server_name(&pod.server)
                ),
            ));
        }
        claim.push((bind, pod));
    }
}

fn sticky(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        let Some(route) = &pod.route else { continue };
        if pod.ingress.knows_client_ip() || route.depth() > MAX_ROUTE_DEPTH {
            continue;
        }
        let sticky = route.nodes().into_iter().any(|node| {
            matches!(
                node,
                Route::Balance {
                    sticky: Some(_),
                    ..
                }
            )
        });
        if sticky {
            out.push(Diagnostic::new(
                Problem::StickyWithoutClientIp,
                vec![Subject::Pod(pod.id.clone())],
                format!(
                    "pod {} keeps clients by address but never learns it: receive the PROXY protocol, or balance after a relay hop",
                    pod.name
                ),
            ));
        }
    }
}

fn unused(index: &Index<'_>, out: &mut Vec<Diagnostic>) {
    for pod in index.pods.values() {
        if pod.route.is_none() && index.out_of(&pod.id).is_empty() {
            out.push(Diagnostic::new(
                Problem::PodWithoutEdges,
                vec![Subject::Pod(pod.id.clone())],
                format!(
                    "pod {} has nowhere to send traffic and is not compiled",
                    pod.name
                ),
            ));
        }
        if pod.ingress.is_relay() && index.edges_into_pod(&pod.id).is_empty() {
            out.push(Diagnostic::new(
                Problem::RelayPodNotDialed,
                vec![Subject::Pod(pod.id.clone())],
                format!(
                    "relay pod {} is not dialed by any pod and is not compiled",
                    pod.name
                ),
            ));
        }
    }
    for exit in index.exits.values() {
        if index.edges_into_exit(&exit.id).is_empty() {
            out.push(Diagnostic::new(
                Problem::ExitNotReached,
                vec![Subject::Exit(exit.id.clone())],
                format!("no edge leads to exit {}", exit.name),
            ));
        }
    }
}
