---
title: Nodes
description: The cards a canvas holds — servers, pods, exits, splitters, aggregators, subcanvases — what each one means and what its panel changes.
---

A canvas holds two kinds of card. **Servers with their pods, exits and subcanvases** are stored:
they are rows the control plane keeps, and editing one is an edit of the graph. **Splitters,
aggregators and portals** are computed from that graph every time the canvas is drawn, to keep a
large fabric readable. This page is the vocabulary; [Canvas](/reference/canvas/) is the surface
these cards sit on and the gestures that edit them.

## The model

| Thing | What it is |
|---|---|
| **Server** | A machine running `guru-worker`, and the set of pods that run on it. It forwards nothing itself. |
| **Pod** | One listener on one server: a port, an optional bind address, an optional advertise address, and its **ingress**. Every pod is its own vertex. |
| **Ingress** | How traffic arrives at a pod. *Client* pods take connections from clients directly — raw TCP, or TLS terminated with an ACME certificate — and may receive a PROXY header. *Relay* pods take traffic other pods relay to them, over TCP, TLS or QUIC. |
| **Exit** | A `host:port` outside the fabric where traffic leaves, optionally with a PROXY header. Any number of pods may lead to one exit. |
| **Edge** | One way a pod's traffic goes on: to a relay pod or to an exit. Two pods may be joined by several edges (to dial different addresses, say). An edge into a pod also says which IP version it dials over: auto, IPv4 or IPv6. |
| **Route** | Each pod's own tree over exactly its out-edges: a **balance** spreads connections over its members by weight, a **failover** uses the first member that is alive. Either may hold the other, to any depth. |

A few rules follow from the model:

- **The protocol of a hop is the target's ingress.** An edge into a `relay_quic` pod is a QUIC hop;
  change the pod's ingress and every pod leading to it dials the new way.
- **The address of a hop** is the edge's override address, else the target pod's advertise
  address, else its server's effective address (learned from the worker: pinned, reported or
  observed; IPv4 when the server has one). An edge set to **IPv4** or **IPv6** dials that version
  instead: the advertise address only when it is of that version, else the server's address of
  it. If the server has none, the pod dialing keeps what it already runs and the canvas warns
  (`dial_family_unreachable`). The port is the edge's override port, else the target pod's port.
- **A client pod cannot be led into**, and a pod may never be reached again by traffic that has
  already passed it: the graph stays acyclic.
- **Ports are picked for you.** A pod saved with no port gets a free one between 40000 and 59999 on
  its server; two pods of one server may not claim the same socket (a wildcard bind overlaps every
  address on that port and transport).
- **Sticky balancing** (by client address) needs a pod that knows the client address: a relay pod,
  or a client pod that receives PROXY.
- **Half-drawn work is legal.** A pod with no way on, a relay pod nothing leads into and an exit no
  edge reaches are warnings; such a pod is simply not deployed, and nothing else is affected.

## Servers

![A server card for guru-test-sg: a Singapore flag, a green Online badge, its IPv4 address, the last report and the worker version, and three client pods — direct and plain over raw TCP, web terminating TLS — with their listen addresses and a blue handle each](/img/canvas/card-server-client.avif)

A server card shows its health badge, the addresses other servers dial it at, the time of its last
health report and the worker version, then one row per pod drawn on this canvas: the colours of the
rules that pass through it, its name, how it listens (a client pod carries an arrow into a box) and
its listen address.

![A server card for guru-test-us-1 with two QUIC relay pods, plain and web, each with a red handle on the left and a blue one on the right](/img/canvas/card-server-relay.avif)

Every pod row has a blue handle on the **right**: the pod's lines leave from it, and dragging from it
gives the pod a new way on. A relay pod's row also has a red handle on the **left**, where the lines
into the pod land and where a way on may be dropped. The red handle in the card's header lands a way
on as a new relay pod of this server. A server of another canvas whose pods are drawn here appears
with a dashed border.

The server's panel holds its settings (name, icon, log level, IPv6 policy, QUIC rates, pinned and
extra addresses), the list of its pods with a row to add one, the agent that runs on it and where it
stands in a rollout.

## Exits

![An exit card: example.com, destination example.com:80, five edges lead here](/img/canvas/card-exit.avif)

An exit card shows its destination, the rules that reach it and how many edges lead there.

## Splitters

![A splitter card: Balance, 2 routes, two members — guru-test-us-1 and example.com — with their weights, and a + member handle](/img/canvas/card-splitter.avif)

A splitter is a group of a route — a balance or a failover — drawn as a card. Groups that choose
the same way between the same cards are drawn as **one** splitter however many pods they belong to:
two rules balanced over the same relay server and the same exit are one splitter standing for two
routes. Each row is one member, with its weight (`×2`) or its tier (`#1`) and where it leads; a
member that is itself a group leads to a nested splitter. The **+ member** handle adds a member to
every route the splitter stands for, and a drag from a member's row gives that member of every route
a way on. Its panel changes the policy, stickiness, weights and order of all of them at once.

## Aggregators

![An aggregator card: 2 servers, one way out, to example.com](/img/canvas/card-aggregator.avif)

Where buses from two or more servers meet in front of the same splitter or exit, an aggregator
gathers them, with one way out per card it hands on to. It is only drawing: nothing about it is
stored, and it cannot be edited or deleted. A drag from one of its ways out, though, gives every pod
on that way a way on at once (see [Connecting](/reference/canvas/#connecting)).

## Subcanvases and portals

A subcanvas is a canvas of the tree drawn as a card inside another; a **portal** card stands for
another canvas of the tree that edges of this one lead into or come from. Both are navigation, not
topology — see [canvas trees](/reference/canvas/#canvas-trees) for what nesting does and does not
change.

## A pod's panel and its route

![The pod panel of plain: port, bind and advertise address, and the route editor showing a balance over the relay pod on guru-test-us-1 and the exit example.com](/img/canvas/panel-route.avif)

Click a pod row to open its panel: its name and comment, how it listens, whether it receives
PROXY, its TLS certificate (SNI, DNS provider, zone or domain id, ACME directory) for a TLS client
pod, its port, bind and advertise address. Below that is its route, as a tree:

- each group has a policy (balance or failover), and a balance may stick to the client address;
- each member of a balance has a weight, each member of a failover its tier;
- the arrows reorder members; tick two or more members of a group to **nest** them as a balance or a
  failover of their own, and **Ungroup** a nested group back into its parent;
- **Add way on** adds a member to that group (the target dialog of
  [Connecting](/reference/canvas/#connecting));
- a way on's menu edits the edge's **dial address** — an override address or port, and the IP
  version it dials over — or removes it.

The shape of the route is a draft until you save it. Adding or removing a way on changes edges and
is written at once, so it waits until the draft is saved or reset. The panel also lists the pods
that lead into a relay pod.

Each pod ends up as one `[[forwarding]]` in its server's config, tagged with the pod's id, and its
route as the upstream table that worker chooses between — see
[Rollout](/reference/rollout/#forwarding-shapes).
