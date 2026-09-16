import { getContext, setContext } from 'svelte';

/**
 * How an edge component opens the side panel on its edge. The count pill of a
 * bundle is portalled out of the edge element by Svelte Flow, so a click on it
 * never reaches the flow's `onedgeclick`; the pill calls this instead. It
 * travels through context like the focused node, for the same reason: folding
 * a callback into edge data would re-render every edge on each refresh.
 */
export type EdgeOpener = {
	open(edgeId: string): void;
};

const KEY = Symbol.for('canvas-edge-opener');

const NONE: EdgeOpener = { open: () => undefined };

export function setEdgeOpener(opener: EdgeOpener): void {
	setContext(KEY, opener);
}

/** Falls back to a no-op for edge components rendered outside a flow. */
export function useEdgeOpener(): EdgeOpener {
	return getContext<EdgeOpener | undefined>(KEY) ?? NONE;
}
