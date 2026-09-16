/**
 * Pure graph → Svelte Flow translation; the node components stay dumb.
 *
 * Split by what each part answers: `ids` names things, `nodes` builds the
 * cards, `ports` says where an edge may attach, `edges` draws them, and
 * `reconcile` merges a refresh into what is already on screen. Importers use
 * this module, not the parts.
 */
export * from './graph/edges.js';
export * from './graph/ids.js';
export * from './graph/nodes.js';
export * from './graph/ports.js';
export * from './graph/reconcile.js';
