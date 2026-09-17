---
title: Canvas
description: The pod graph a canvas draws — pods, exits, edges and routes — how to read it, and every gesture that edits it.
---

A canvas tree holds one **pod graph**: a directed acyclic graph whose vertices are listeners on your
servers and whose sinks are destinations outside the fabric. The control plane checks the whole
graph on every edit and derives one config per server from it (see
[Rollout Model](/reference/rollout/)). Everything else the canvas shows — splitters, aggregators,
buses — is how that graph is *drawn*, computed from it on the fly.

![The main canvas: two client pods on a Hangzhou server each send their own line into one splitter, which fans out to relay pods on five Hong Kong servers; an aggregator gathers their lines into two exits](/img/canvas/canvas-overview.avif)

## The model

| Thing | What it is |
|---|---|
| **Server** | A machine running `guru-worker`, and the set of pods that run on it. It forwards nothing itself. |
| **Pod** | One listener on one server: a port, an optional bind address, an optional advertise address, and its **ingress**. Every pod is its own vertex. |
| **Ingress** | How traffic arrives at a pod. *Client* pods take connections from clients directly — raw TCP, or TLS terminated with an ACME certificate — and may receive a PROXY header. *Relay* pods take traffic other pods relay to them, over TCP, TLS or QUIC. |
| **Exit** | A `host:port` outside the fabric where traffic leaves, optionally with a PROXY header. Any number of pods may lead to one exit. |
| **Edge** | One way a pod's traffic goes on: to a relay pod or to an exit. Two pods may be joined by several edges (to dial different addresses, say). |
| **Route** | Each pod's own tree over exactly its out-edges: a **balance** spreads connections over its members by weight, a **failover** uses the first member that is alive. Either may hold the other, to any depth. |

A few rules follow from the model:

- **The protocol of a hop is the target's ingress.** An edge into a `relay_quic` pod is a QUIC hop;
  change the pod's ingress and every pod leading to it dials the new way.
- **The address of a hop** is the edge's override address, else the target pod's advertise
  address, else its server's effective address (learned from the worker: pinned, reported or
  observed). The port is the edge's override port, else the target pod's port.
- **A client pod cannot be led into**, and a pod may never be reached again by traffic that has
  already passed it: the graph stays acyclic.
- **Ports are picked for you.** A pod saved with no port gets a free one between 40000 and 59999 on
  its server; two pods of one server may not claim the same socket (a wildcard bind overlaps every
  address on that port and transport).
- **Sticky balancing** (by client address) needs a pod that knows the client address: a relay pod,
  or a client pod that receives PROXY.
- **Half-drawn work is legal.** A pod with no way on, a relay pod nothing leads into and an exit no
  edge reaches are warnings; such a pod is simply not deployed, and nothing else is affected.

## Reading the canvas

### Servers

![A server card for 杭州移动: a Chinese flag, a green Online badge, its IPv4 and IPv6 addresses, the last report, and two client pods m_hk7_145 and tutusgak with their listen addresses and a blue handle each](/img/canvas/card-server-client.avif)

A server card shows its health badge, the addresses other servers dial it at, the time of its last
health report and the worker version, then one row per pod drawn on this canvas: the colours of the
rules that pass through it, its name, how it listens (a client pod carries an arrow into a box) and
its listen address.

![A server card for gcore-hk-1 with two QUIC relay pods, each with a red handle on the left and a blue one on the right](/img/canvas/card-server-relay.avif)

Every pod row has a blue handle on the **right**: the pod's lines leave from it, and dragging from it
gives the pod a new way on. A relay pod's row also has a red handle on the **left**, where the lines
into the pod land and where a way on may be dropped. The red handle in the card's header lands a way
on as a new relay pod of this server. A server of another canvas whose pods are drawn here appears
with a dashed border.

The server's panel holds its settings (name, icon, log level, IPv6 policy, QUIC rates, pinned and
extra addresses), the list of its pods with a row to add one, the agent that runs on it and where it
stands in a rollout.

### Exits

![An exit card: exit-8443, destination localhost:8443, five pods lead here](/img/canvas/card-exit.avif)

An exit card shows its destination, the rules that reach it and how many edges lead there.

### Splitters

![A splitter card: Balance, 2 routes, five members gcore-hk-1 to gcore-hk-5 with their weights, and a + member handle](/img/canvas/card-splitter.avif)

A splitter is a group of a route — a balance or a failover. Groups that choose the same way between
the same cards are drawn as **one** splitter however many pods they belong to: two rules fanned out
over the same five servers are one splitter standing for two routes. Each row is one member, with
its weight (`×2`) or its tier (`#1`) and where it leads; a member that is itself a group leads to a
nested splitter. The **+ member** handle adds a member to every route the splitter stands for, and
a drag from a member's row gives that member of every route a way on. Its panel changes the policy,
stickiness, weights and order of all of them at once.

### Aggregators

![An aggregator card: 5 servers, two ways out, to exit-8443 and to 出口 黑色的海豚](/img/canvas/card-aggregator.avif)

Where buses from two or more servers meet in front of the same splitter or exit, an aggregator
gathers them, with one way out per card it hands on to. It is only drawing: nothing about it is
stored, and it cannot be edited or deleted. A drag from one of its ways out, though, gives every pod
on that way a way on at once (see *Connecting* below).

### Buses and rules

A **bus** is every edge that takes the same way between two handles, drawn as one cable with a thin
line inside it per **rule** in the rule's colour, and a count when it stands for several edges. Lines
leave at blue handles and land at red ones: a pod's lines start at its own row and end at the row of
the relay pod they dial, or at a splitter, an aggregator, an exit or a subcanvas. A line between two
pods of one server loops round under the card.

A rule is a client pod — where traffic enters the fabric — and it colours everything its traffic
can pass through. The legend in the top-right corner lists the rules of the canvas; clicking one
fades everything that does not carry it.

Click a bus to open its panel: where it runs, the rules riding it — each in its line's colour, with
how many of the bus's edges carry it — and every edge with its id, the rules it carries, where it
leads and the address it dials.

![The panel of the line from m_hk7_145 into the splitter: from m_hk7_145 · 杭州移动 to Balance, one rule in green riding 5 edges, and the five edges to the relay pods on gcore-hk-1 to gcore-hk-5, each with its edge ID](/img/canvas/panel-bus.avif)

### Subcanvases and portals

A subcanvas is a canvas drawn inside another; double-click it to go inside. Nesting only organises
the drawing: edges cross canvas boundaries freely. A connection dropped on a subcanvas card asks
which relay pod, exit or server inside it is meant, and a **portal** card stands for another canvas
of the tree that edges of this one lead into or come from.

### Problems

The corner at the bottom left lists what the control plane finds wrong with the tree's graph —
problems concerning this canvas first, the rest of the tree after. Click one to open what it is
about, on its own canvas when it lives elsewhere. An error blocks the next edit; a warning never
does.

## Editing

Every edit is **one batch** — the rows it puts and deletes — that the control plane checks as a
whole against the graph it leads to and writes all of or none of. A batch computed against a tree
that has changed since the canvas read it is refused; the canvas reloads and you try again.

### Adding

**Add** in the menu bar creates a server, an exit (asking for its destination) or a subcanvas, near
the middle of the view and clear of other cards. Pods are added in a server's panel: a name, how the
pod listens, and a port — leave it empty for one picked for you.

### Connecting

Drag from the blue handle on the right of a pod row and drop it on a red one:

| Drop on | Result |
|---|---|
| a relay pod's row | an edge to that pod |
| an exit | an edge to the exit |
| a server card's header | a new relay pod on that server, listening in the protocol you choose |
| a splitter | the pod joins it: its route gains a copy of the splitter's group, landing on relay pods of its own on the same servers |
| a subcanvas or a portal | a relay pod, an exit or a new relay pod on a server inside it, as you pick |

A new way on joins the pod's route: the first one is the route, a second one makes a balance of the
two, and a group gains a member. Dragging from a splitter's **+ member** handle does the same for
every route the splitter stands for.

The rows of drawn cards can be dragged from too, and one drop reaches every pod behind the row:

| Drag from | What gains a way on |
|---|---|
| a splitter's member row | that member of each route the splitter stands for |
| an aggregator's way out | each pod whose line runs through that way |

A row takes the drops a pod row takes, except a splitter, a place it already leads to and its own
pods. As with one pod, a single edge becomes a balance of the old and the new, and a group gains a
member; make it a failover in the splitter's panel. Dropped on a server's header from a row or from
**+ member**, the pods of one rule share the one new relay pod made there: an aggregator's way out
dragged to a new exit or server connects five relay pods of a rule in one go, and one splitter
appears.

Connecting a relay pod that had no way on, when the other relay pods its group landed have none
either, shows a notice with a button that connects them the same way.

### Editing a pod

![The pod panel of m_hk7_145: port, bind and advertise address, and the route editor showing a balance over relay pods on gcore-hk-1 and gcore-hk-2](/img/canvas/panel-route.avif)

Click a pod row to open its panel: its name and comment, how it listens, whether it receives
PROXY, its TLS certificate (SNI, DNS provider, zone or domain id, ACME directory) for a TLS client
pod, its port, bind and advertise address. Below that is its route, as a tree:

- each group has a policy (balance or failover), and a balance may stick to the client address;
- each member of a balance has a weight, each member of a failover its tier;
- the arrows reorder members; tick two or more members of a group to **nest** them as a balance or a
  failover of their own, and **Ungroup** a nested group back into its parent;
- **Add way on** adds a member to that group (the target dialog of *Connecting*);
- a way on's menu edits the edge's **dial address** — an override address or port — or removes it.

The shape of the route is a draft until you save it. Adding or removing a way on changes edges and
is written at once, so it waits until the draft is saved or reset. The panel also lists the pods
that lead into a relay pod.

### Removing

Select cards or buses and press **Delete**, or use the delete button of a panel. What goes is shown
before it goes, with the control plane's verdict on the graph it leads to:

![The review dialog: 2 pods removed, 4 edges removed, 2 pods rewritten, a switch to also remove relay pods left unreached, and the control plane accepts this change](/img/canvas/dialog-review.avif)

Removing a pod takes every edge into and out of it, and rewrites the routes that named them.
**Also remove relay pods left unreached** takes away the relay pods nothing leads into any more,
with the ways on out of them — what landed for a rule on its transit servers. Deleting a server
removes its pods first; deleting a subcanvas deletes everything in it, and is refused while a pod
outside still leads into it or runs on a server inside it.

### Moving

Drag cards to move them; positions are saved when you drop them. Servers, exits and subcanvases
keep their positions on their own rows. Splitters, aggregators, portals and servers of other
canvases are remembered in the canvas's layout, and an edit keeps every card where it was: a
splitter an edit reshapes stays in place, and a new card is placed clear of the others.

## How an edit is checked

These refuse a batch:

| Problem | Cause |
|---|---|
| `cycle` | Traffic could come back to a pod it has already passed |
| `route_missing_edge`, `route_duplicate_edge`, `route_unknown_edge`, `route_foreign_edge`, `missing_route` | A route that does not name each of the pod's out-edges exactly once |
| `empty_group`, `zero_weight`, `route_too_deep` | A group without members, a weight of zero, nesting deeper than 32 |
| `client_pod_dialed` | An edge into a client pod |
| `listener_conflict` | Two pods of one server on the same port and transport with overlapping binds |
| `listener_in_use` | A port some server still dials in another protocol: give the pod a new port instead |
| `sticky_without_client_ip` | A sticky balance on a pod that never learns the client address |
| `invalid_bind_ip`, `invalid_advertise_ip`, `invalid_override_address`, `invalid_exit_destination`, `invalid_sni` | Addresses, destinations and names that do not parse |
| `unknown_dns_provider`, `certificate_issued_elsewhere` | A TLS pod naming a missing DNS provider, or disagreeing with another pod on how its SNI is issued |
| `no_free_port` | Every port between 40000 and 59999 is taken on the server |
| `edge_ends_changed` | An edge's ends are its identity: remove it and add another |

These are only warnings: `single_tier_failover`, `pod_without_edges`, `relay_pod_not_dialed`,
`exit_not_reached`.

## What a worker runs

Each pod becomes one `[[forwarding]]` in its server's config, tagged with the pod's id. A worker
that reports the `route_table` capability receives the route as a table of groups and upstreams and
chooses between members by what is alive; a relay hop toward such a worker asks the relay to
confirm that its own next hop answered, so a dead exit behind a live relay moves the choice on at
the dialer. An older worker receives the route in the tree form. See the
[configuration reference](/reference/configuration/#forwardingto) for both.
