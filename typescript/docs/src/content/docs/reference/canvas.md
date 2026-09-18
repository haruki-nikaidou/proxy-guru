---
title: Canvas
description: The pod graph a canvas holds, how the canvas draws it, and how every edit is batched and checked.
---

A canvas tree holds one **pod graph**: a directed acyclic graph whose vertices are listeners on your
servers and whose sinks are destinations outside the fabric. The control plane checks the whole
graph on every edit and derives one config per server from it (see [Rollout](/reference/rollout/)).
The canvas is how that graph is drawn and the only place it is edited; the cards it draws are
described in [Nodes](/reference/nodes/).

![The main canvas: three client pods on a Singapore server; two of them send their own line into one splitter, which balances between a relay pod on a US server and the exit itself, while the third goes straight to the exit; an aggregator gathers the lines from both servers into the exit example.com](/img/canvas/canvas-overview.avif)

## Stored and drawn

What the control plane keeps is small: servers with their pods, exits, subcanvases, one edge per way
on, one route tree per pod, and where each card sits. Everything else in the picture is computed
from that graph while it is drawn, so that a fabric of forty edges stays a handful of cards.

![Two panels showing the same topology. Above, what is stored: a Singapore server with the client pods plain, web and direct, a US server with the relay pods plain and web, the exit example.com:80, seven edges between them, and a balance route on each of the two pods that have two ways on. Below, what is drawn: the same servers and exit, but the lines out of plain and web enter one Balance splitter standing for 2 routes, the cable from it to the US server is one bus of 2 edges, and the lines in front of the exit meet in one aggregator over 2 servers](/img/canvas/graph-vs-drawing.svg)

Splitters stand for route groups, aggregators for lines that meet, buses for edges that run the same
way. None of them has a row of its own: they appear, merge and vanish as the graph changes, which is
why they cannot be deleted and why one splitter card may belong to many pods at once.

## Buses and rules

A **bus** is every edge that takes the same way between two handles, drawn as one cable with a thin
line inside it per **rule** in the rule's colour, and a count when it stands for several edges. Lines
leave at blue handles and land at red ones: a pod's lines start at its own row and end at the row of
the relay pod they dial, or at a splitter, an aggregator, an exit or a subcanvas. A line between two
pods of one server loops round under the card.

A rule is a client pod — where traffic enters the fabric — and it colours everything its traffic
can pass through. The legend in the top-right corner lists the rules of the canvas; clicking one
fades everything that does not carry it.

Click a bus to open its panel: where it runs, the **IP version** its edges into pods dial over —
auto, IPv4 or IPv6, set for all of them at once (*Mixed* while they differ) — what the control plane
finds wrong with its edges, the rules riding it — each in its line's colour, with how many of the
bus's edges carry it — and every edge with its id, the rules it carries, where it leads and the
address it dials.

![The panel of the line from plain into the splitter: from plain · guru-test-sg to Balance, one rule riding 2 edges, and the two edges — to the relay pod plain on guru-test-us-1 and to example.com — each with its edge ID](/img/canvas/panel-bus.avif)

## Canvas trees

A canvas may be drawn inside another as a subcanvas card; double-click it to go inside. Nesting only
organises the drawing: edges cross canvas boundaries freely, and the graph the control plane checks
and derives from is the whole tree, not one canvas. A connection dropped on a subcanvas card asks
which relay pod, exit or server inside it is meant, and a canvas reached from elsewhere in the tree
appears as a portal card.

## Problems

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
| a splitter | the pod joins it: its route gains a copy of the splitter's group, landing on relay pods of its own on the same servers. Those have no way on yet: connect one, and the notice offers to connect the rest |
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

A pod's own route — the shape of the groups its ways on hang in, their weights and their dial
addresses — is edited in [the pod's panel](/reference/nodes/#a-pods-panel-and-its-route).

### Removing

Select cards or buses and press **Delete**, or use the delete button of a panel. What goes is shown
before it goes, with the control plane's verdict on the graph it leads to:

![The review dialog: 2 pods removed, 4 edges removed, 2 pods rewritten, server guru-test-us-1 deleted, a switch to also remove relay pods left unreached, and the control plane accepts this change](/img/canvas/dialog-review.avif)

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
`exit_not_reached`, and `dial_family_unreachable` — edges set to IPv4 or IPv6 whose target server
has no address of that version, or whose target pod listens on the other version only. The pods
dialing over it keep what they already run until it can be reached.

An accepted batch bumps the tree's generation, and the control plane derives a new config for every
server the change touches — what happens next is [Rollout](/reference/rollout/).
