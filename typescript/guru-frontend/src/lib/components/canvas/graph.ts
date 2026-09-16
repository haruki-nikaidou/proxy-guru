/**
 * Pure graph → Svelte Flow translation; the node components stay dumb.
 *
 * Split by what each part answers:
 * - `ids` — how servers, nodes and handles are named in the flow's id space
 * - `nodes` — the cards, the problem level each carries, and what the side
 *   panel is pointed at
 * - `ports` — where an edge may attach, and whether a drag is allowed
 * - `edges` — the edges as drawn, their colours, and what the panel says about one
 * - `reconcile` — merging a refresh into what is already on screen
 * - `pod-ports` — the TCP port a new pod should listen on (not a flow handle)
 *
 * Importers use this module, not the parts.
 */
export * from './graph/edges.js';
export * from './graph/ids.js';
export * from './graph/nodes.js';
export * from './graph/pod-ports.js';
export * from './graph/ports.js';
export * from './graph/reconcile.js';
