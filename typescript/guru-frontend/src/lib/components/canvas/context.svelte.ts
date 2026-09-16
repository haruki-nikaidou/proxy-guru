import { getContext, setContext } from 'svelte';
import type { PanelTarget } from './flow/nodes.js';

/**
 * What the cards and buses read from the canvas they are drawn on, beyond their
 * own data: which card the side panel is editing, how to open the panel, and the
 * rule palette. It travels through context rather than node data: the flow
 * mirror is reconciled against the server graph, and folding purely local state
 * into it would re-render (and re-measure) every card on each click.
 */
export type CanvasContext = {
	/** The card id the panel's target is drawn on, or `null`. */
	readonly focusedCard: string | null;
	/** The pod the panel is editing, for its row to stand out. */
	readonly focusedPod: string | null;
	/** The rule the legend singles out; everything else fades. */
	readonly highlightedRule: string | null;
	open(target: PanelTarget): void;
	/** The CSS colour of a rule. */
	ruleColor(rule: string): string;
	ruleName(rule: string): string;
};

const KEY = Symbol.for('guru-canvas-context');

const NONE: CanvasContext = {
	focusedCard: null,
	focusedPod: null,
	highlightedRule: null,
	open: () => undefined,
	ruleColor: () => 'var(--canvas-bundle)',
	ruleName: rule => rule
};

export function setCanvasContext(context: CanvasContext): void {
	setContext(KEY, context);
}

/** Falls back to an inert context for cards rendered outside a flow. */
export function useCanvasContext(): CanvasContext {
	return getContext<CanvasContext | undefined>(KEY) ?? NONE;
}

/** The CSS colour of palette slot `slot`: `--channel-0` … `--channel-11`. */
export const paletteColor = (slot: number): string => `var(--channel-${slot})`;
