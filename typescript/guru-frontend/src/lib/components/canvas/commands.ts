/**
 * The canvas editor's remote functions live with their route (SvelteKit requires
 * `*.remote.ts` under `src`), while the components live in `#lib`. This module is
 * the single place that bridges the two, so no component carries a deep relative
 * path — and no component has to know which of the route's files a call is in.
 */
export * from '../../../routes/(canvas)/canvas/[canvasId]/graph.remote.js';
export * from '../../../routes/(canvas)/canvas/[canvasId]/servers.remote.js';
export * from '../../../routes/(canvas)/canvas/[canvasId]/subcanvas.remote.js';
