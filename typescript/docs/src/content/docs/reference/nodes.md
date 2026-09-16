---
title: Nodes
description: Every node kind on the canvas — what the card shows, which handles it has, what you may edit, and what it becomes on the wire.
---

A canvas is a graph of **nodes** joined by **edges**. Nodes carry no traffic themselves: the control
plane reads the graph, derives one config per server from it, and ships that config to the workers
(see [Rollout Model](/reference/rollout/)). This page is the catalogue — one section per kind, with
the card as the dashboard draws it.

## Reading a card

Every node renders the same chrome: an icon, the node **name**, an optional badge, and the **kind**
in the top-right corner. Under the header sits a summary line built from the node's own
configuration, then its handles. An operator comment, if set, shows as a muted line below the
header.

New nodes get a `<colour>-<animal>` name (`Entry harlequin-weasel`, `Exit moccasin-gorilla`) drawn
from a colour and animal dictionary — never an adjective, because words like *broken* read as
status. The name is just a label: rename it to anything.

A **warm orange ring** means the node is the one open in the inspector (this is the selection
outline in the Exit screenshot below). A red ring means the node has a validation error, an amber
ring a warning; the inspector ring deliberately wins over both while a node is selected.

### Handles

Each handle is one **port**. A port has a kind and a direction, and both are generated from the node
kind — you never invent port names.

| Appearance | Port kind | Meaning |
|---|---|---|
| Blue circle | `listen` | A listener: this end accepts connections |
| Olive circle | `destination` | A destination: this end dials onwards |
| Circle in a channel colour | `destination` | The port belongs to one channel; the colour identifies the channel along its whole path |
| Grey square | `bundle` | Not traffic — expansion metadata carrying every channel of a node to the next one |

Three rules cover every connection:

- **Outputs sit on the left edge of a card, inputs on the right.** A chain therefore reads
  right-to-left: a pod's `listen` output (left) feeds an Entry's `listen` input (right).
- **Kinds must match.** Blue joins blue, olive joins olive, square joins square. A listen port never
  connects to a destination port.
- **A port takes exactly one edge.** Once wired, the handle stops accepting drags. Only an "add"
  node's *group* handles accept many edges.

The dashboard refuses invalid drags as you make them, and the server re-checks the whole topology on
every write, so an edit either lands complete or is rejected.

Half-drawn work is legal and is not an error: a relay nobody feeds, a load balancer with two of its
four members connected, an exit with no destination yet. Such a pod is simply not derived, and every
other pod on the server is unaffected.

## Entry

![An Entry node card titled "Entry harlequin-weasel", summary line "Receive PROXY protocol: None", and one blue listen handle](/img/nodes/node-entry.avif)

The ingress edge of the fabric: an Entry describes *how* a listener accepts client connections.

| Handle | Kind | Direction |
|---|---|---|
| `listen` | listen (blue) | input — fed by exactly one pod |

- **Receive PROXY protocol** — `None`, `PROXY v1` or `PROXY v2`, shown verbatim on the summary line.
  With `None`, the real client IP is unknown, which makes an `ip_hash` load balancer anywhere below
  that pod an error.
- **TLS** — off means the pod listens raw; on means the pod terminates TLS with an ACME certificate.
  A TLS entry needs an SNI (a plain hostname: at least two labels, no wildcard), a DNS provider, the
  provider's domain/zone id, and optionally an ACME directory URL (empty uses the configured
  default). When TLS is on, the card shows a lock badge with the SNI. Certificates are keyed by
  `(SNI, ACME directory)`, so every Entry sharing an SNI must agree on provider and domain id; until
  the certificate is issued, that one pod stays invalid.

## Exit

![An Exit node card titled "Exit moccasin-gorilla" with an orange selection ring, summary line "Not set", and one olive destination handle](/img/nodes/node-exit.avif)

The terminal hop: traffic leaves the fabric here.

| Handle | Kind | Direction |
|---|---|---|
| `destination` | destination (olive) | output |

- **Destination** — a `host:port` remote. Empty shows `Not set` and raises a warning; a non-empty
  value that does not parse is an error.
- **Pass PROXY protocol** — send PROXY v1/v2 to the remote, or nothing.

## Relay

![A Relay node card titled "Relay moccasin-rooster", summary line "TCP (raw)", a blue listen handle and an olive destination handle](/img/nodes/node-relay.avif)

A hop between two of your own servers. A Relay is a *pair* of ends drawn as one node: its `listen`
input is fed by the pod that **accepts** the relay connection, and its `destination` output feeds the
pod that **dials** it.

| Handle | Kind | Direction |
|---|---|---|
| `listen` | listen (blue) | input |
| `destination` | destination (olive) | output |

- **Relay protocol** — `TCP (raw)`, `TCP (TLS)` or `QUIC`. TLS and QUIC use the fabric's internal CA
  and a per-pod leaf certificate; raw TCP auto-detects PROXY on ingest, so a relay hop always knows
  the client IP.
- **Override IP address / Override port** — leave empty to use the derived value. The dialled address
  is the override, else the target pod's advertise address, else the target server's effective
  address; the port is the override, else the target pod's port. Both overrides are appended to the
  summary line.

Both ends landing on the same server is a warning — you almost certainly meant two servers.

## Load balance (distribute)

![A Distribute node card titled "fan-out", summary line "Round robin · QUIC · Members: 4", three coloured channel handles named after their entry pods plus faint "+ bundle" and "+ channel" handles on the left, and four square member handles named hk-1 to hk-4 on the right](/img/nodes/node-distribute.avif)

Fans every **channel** bundled through it over its **members** (see
[Channels and bundles](#channels-and-bundles)). The members are the rule you write: one per transit
server, named by you, one bundle out each. Four AWS boxes are four members; a fifth is one more
member and one more bundle.

| Handle | Kind | Direction |
|---|---|---|
| one per member, your name | bundle (grey square) | source — one edge, to a universal pod's or a distribute node's `+ bundle` |
| one per incoming bundle | bundle (grey square), named after the far node | target — one edge, from a universal pod's `bundle out` or another distribute node's member |
| one per channel | destination, channel colour | source — one edge, to the `destination` of the entry pod that is the channel |
| `+ bundle` | bundle (grey square), add | target — where an upstream bundle is dropped: every channel it carries is fanned out again from here |
| `+ channel` | destination (olive), add | source — drag to the `destination` of an entry pod; the connected pod becomes one more coloured channel |

- **Balance mode** — `Round robin`, `Random`, `IP hash` or `Fallback`. `IP hash` requires a known
  client IP, so it is an error under an Entry that receives no PROXY protocol.
- **Relay protocol** — how the channels this node fans out are relayed to the universal pods its
  members are bundled to (`TCP (raw)`, `TCP (TLS)`, `QUIC`). A hop nothing states a protocol for
  (an entry pod drawn straight into a universal pod, a universal pod bundled to the next one) is
  raw TCP. Changing it gives every landing pod a new port.
- **Members** — 1 to 256, each with a name of your own (unique, at most 64 characters). The
  inspector lists them with the far end of each bundle; add one with *Add member*, remove one with
  its bin. A member keeps its handle and its bundle when renamed or reordered; removing a member
  that still carries a bundle is refused until the bundle is cut. Exactly one member bundled is a
  warning. There are no weights — fan-out is per member.

Bundled *into*, a distribute node fans out again everything the bundles carry — the second tier of
a fan-out (four transit servers spreading over two landing servers), or, bundled to from another
distribute node, a nested strategy (a `Fallback` node whose members are two `Round robin` groups).
Each upstream path gets lanes of its own on the target servers.

A channel must start at a **pod's** destination handle; anything else is refused. The channel list
under the members shows one coloured chip per channel.

## Load balance (aggregate)

![An Aggregate node card titled "join", summary line "Members: 4", four square member handles named hk-1 to hk-4 on the left, and four coloured channel outputs named after their entry pods on the right](/img/nodes/node-aggregate.avif)

The mirror of distribute: where the bundles meet again. Its **members** are the bundles it joins,
one per transit server, named by you; bundled in, it grows **one output per channel** the bundles
carry, each waiting for an exit. It contributes nothing of its own to the derived config.

| Handle | Kind | Direction |
|---|---|---|
| one per member, your name | bundle (grey square) | target — one edge, from a universal pod's `bundle out` |
| one per channel | destination, channel colour | output — connect an exit to each |

**Members** (1–256, named) is the only setting; an aggregate has no balance mode, and the
distribute ↔ aggregate choice is fixed when the node is created. Renaming keeps a member's bundle;
removing a member that still carries one is refused. Its inspector lists each channel with the
exit feeding it, or `no exit`; until a universal pod is bundled onto a member, the card shows *No
channels yet*.

## Server

![A Server node card titled "hk-1" with an Online badge, address and last-seen lines, a Universal pod section with two channel dots, one square bundle handle named fan-out plus a faint "+ bundle" handle on the left and a square bundle out handle on the right, and "No pods yet"](/img/nodes/node-server.avif)

One machine running `guru-worker`. A server is not a node spec but a container: it holds every
**pod** on that machine plus the machine's **universal pod**. Pods never appear as separate cards.

**Icon.** Left empty, the header shows the flag of the country the server's IPv4 address is in:
the master looks each address up once through `country_lookup_url` (see the
[configuration reference](/reference/configuration/#module-configuration)) and again when it
changes, so the flag follows the card's IPv4 line. Whatever you type overrides it, from two icon
sets behind short prefixes: `flag:<code>` for a country flag (`flag:us`, `flag:jp`) and
`logo:<name>` for a brand logo (`logo:tauri`, `logo:svelte`). Names come from
[circle-flags](https://icon-sets.iconify.design/circle-flags/) and
[theSVG Color](https://icon-sets.iconify.design/thesvg-color/); the icon itself is fetched on demand
from the Iconify API, so an air-gapped browser keeps the default glyph. Anything that does not
resolve — a raw Iconify name, an unknown prefix, a name the set does not have, an empty field whose
country is not known yet — renders the default server glyph, and the inspector previews the result
next to the field while you type.

The header badge is health, filled like a traffic light: a green `Online`, an amber `Degraded`
(apply error, failed pods, or a revision lagging past the grace period), a red `Offline` (nothing
reported for three health intervals — 45 s by default), or an outlined `Unknown`. Under the header,
one line holds the server's IPv4 address and one its IPv6 address (each the first of pinned,
reported and observed, see below, or `no address yet`), then the time of the last health report,
with `not reporting` appended while the server is offline. The settings are the inspector's, not
the card's: the log level, the IPv6 resolution policy (`Required`/`Preferred`/`Tolerated`/`Forbidden`,
default `Tolerated`) and the QUIC rates below.

**QUIC relay links.** The inspector's last group is this server's side of every QUIC relay link it
takes part in: the congestion control (`Cubic`, or `Brutal`, which sends at exactly the up rate
whatever the path does), the up and down rates in Mbit/s, and two optional receive windows in bytes
(0 = derived from the down rate / unlimited). They become the worker's `[quic]` section; on each link
the master pairs the two ends, so a server never sends faster than its peer's down rate (see the
[configuration reference](/reference/configuration/#quic)). `Brutal` needs an up rate.

Nobody types a server's address: the worker reports its IPv4/IPv6 on registration and the master
remembers where the registration came from. Precedence is pinned v4 → reported v4 → observed v4 →
pinned v6 → reported v6 → observed v6. `no address yet` is a warning on every pod of that server —
its own pods still derive; only *other* servers dialling it stay invalid.

**Pods.** A pod is one listening socket: a name, a port, an optional bind address (empty binds every
address of the host, rendered `[::]:port`) and an optional advertise address (empty uses the
server's effective address). Each pod row carries two handles:

| Handle | Kind | Direction |
|---|---|---|
| `listen` | listen (blue) | output — offers the listener to an Entry or a Relay |
| `destination` | destination (olive) | input — consumes a destination subtree |

Two pods of one server may not claim the same socket; a wildcard bind clashes with every literal on
that port. A pod with either handle unconnected is simply not derived — that is the normal state
while you are wiring.

One pod becomes one `[[forwarding]]` entry in the server's TOML, tagged with the pod name. A pod that
fails to derive on its own shows up in the server's rollout panel as an invalid pod with the reason,
and never disturbs the rest of the config.

**Universal pod.** Created with the server, one per server, and neither creatable nor deletable by
hand. It is where other servers' traffic lands without drawing a pod per rule: one grey square per
incoming bundle (named after where it comes from), one coloured circle per entry pod drawn straight
into it (a raw TCP hop of its own, no balancing), the faint `+ bundle` / `+ channel` handles
accepting the next, and the single `bundle out` (grey square, exactly one edge) that hands the
bundle on to the next universal pod, to a distribute node or onto a member of a load balance
(aggregate) node. Every channel a bundle carries gets a real **landing pod** here, listed in the
server's inspector with an editable port. The header of the section shows one dot per channel
(rule) landing here and counts both channels and landing pods: a rule that reaches the server by
two paths (say, directly and through a second tier) lands twice and is joined here, so it is one
channel with two pods.

## Channels and bundles

A **channel** is one rule — one entry pod — travelling from a load balance (distribute) node,
through the universal pods of your transit servers, to a load balance (aggregate) node, with its
own colour from end to end. A **bundle** is one thick grey line carrying every channel to the next
hop; the edge label counts them. The picture for ten rules over four transit servers is ten entry
pods into one distribute node, four bundles out of it, four bundles into one aggregate node, and
ten coloured lines to ten exits.

Bundles are pure canvas sugar. Before anything is derived, they are **expanded** into ordinary nodes
("lanes"): per channel and per target server, a landing pod, a relay speaking the distribute node's
protocol, a distribute lane when there is more than one target, and an aggregate lane where several
landing pods of one channel meet. Nothing about a bundle exists on the wire, and derivation,
convergence and health only ever see the expanded graph.

Lane nodes are managed: they cannot be retired or re-wired, and only a landing pod's port and
addresses may be edited. A lane keeps its identity across edits, so it keeps its port, its row and
its health history.

A bundle always leaves through a handle that is already there — a distribute node's member or a
universal pod's `bundle out` — and is collected on the far side: a universal pod or a distribute
node grows a square per bundle dropped on its `+ bundle`, an aggregate node takes it on one of its
members. Legal bundles are member → universal pod, member → distribute (nesting), universal pod →
universal pod, universal pod → distribute (the next tier), and universal pod → aggregate member.
Distribute → aggregate is refused: bundle the distribute node to your transit servers' universal
pods first. Two members of one node never bundle to the same far node. A bundle cycle is an error;
a channel bundled to no server, or landing somewhere and going nowhere, is a warning.

## Export

![Four Export cards: "listen · Out of this canvas" and "destination · Out of this canvas" on the left, "listen · Into this canvas" and "destination · Into this canvas" on the right](/img/nodes/node-export.avif)

One boundary port of the canvas it sits on. The parent canvas sees it as a handle on the subcanvas
node, labelled with the export's name.

An export has exactly one handle, `export`, and two settings that pick which of the four variants it
is:

| Port kind | Direction | Handle inside this canvas |
|---|---|---|
| `listen` | Into this canvas | blue output |
| `listen` | Out of this canvas | blue input |
| `destination` | Into this canvas | olive output |
| `destination` | Out of this canvas | olive input |

*Into this canvas* means traffic arrives from the parent and is emitted here, which is why it is an
**output** on the inside. Both settings are asked when the node is created, because changing either
one later reshapes the parent's handle and **drops the edge attached to it**. Moving an export
vertically reorders the parent's handles.

An export is not a traffic vertex: validation and derivation resolve straight through it.

## Subcanvas

![A Subcanvas card titled "Subcanvas amber-…" listing four handles named after the child canvas's export nodes](/img/nodes/node-subcanvas.avif)

Embeds another canvas as one card. Its handles are derived, one per export node of the embedded
canvas — kind copied, direction **mirrored**, ordered by the export's vertical position, labelled
with the export's name. So an export that is `listen · Into this canvas` (an output inside the
child) appears on the parent card as a blue input.

Double-click the card to descend into the child canvas.

The embedded canvas cannot be changed: delete the node and import again. A canvas may be imported at
most once, and never by itself or by one of its own descendants. An imported canvas is hidden from
the canvas list unless you ask for subcanvases, and cannot be deleted while it is imported; retiring
the subcanvas node frees it back to being a root canvas of its own.

A canvas tree is validated and derived as **one flat graph**: a pod may live on any server of the
tree, and edits inside a child reshape the parent's handles in the same transaction.

## Problems

Validation runs over the whole tree on every write and reports two severities. Errors block the
edit; warnings are informational and never stop you from saving.

**Errors** — mismatched port kinds, an edge that is not output → input, a self-loop, a cross-canvas
edge, an oversubscribed port, an invalid port shape, a cycle, two pods claiming one socket, a pod on
a server outside the tree, an unparsable exit destination, `ip_hash` without a client IP, a channel
that does not start at a pod, an invalid or cyclic bundle, and the four subcanvas nesting mistakes
(self, ancestor, duplicate, unresolved).

**Warnings** — an exit with no destination yet, a server with no address yet, a relay whose ends
share a server, a distribute group with a single connected member, a channel with no transit or no
exit, and stale lanes (the next edit of a bundled node regenerates them).
