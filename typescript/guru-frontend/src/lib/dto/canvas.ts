export type CanvasProblem = {
	severity: 'error' | 'warning';
	/** The diagnostic's stable name, for grouping only — always display `message`. */
	kind: string;
	message: string;
};

/** The identity of a canvas: what the switcher and settings page work with. */
export type CanvasOption = {
	id: string;
	name: string;
	description: string;
};

/** A canvas as the switcher lists it; `isRoot` is false for a subcanvas. */
export type CanvasOptionEntry = CanvasOption & { isRoot: boolean };

export type CanvasSummary = CanvasOption & {
	/** null when reading the canvas's tree failed */
	stats: { servers: number; pods: number; edges: number } | null;
	/** null on the same failure; `problems` is capped at 5 entries. */
	health: { errors: number; warnings: number; problems: CanvasProblem[] } | null;
	/** The canvas this one is drawn inside; null for a root. */
	parent: CanvasOption | null;
};

/** One canvas of a nesting tree, as `GetCanvasTree` returns it. */
export type CanvasTreeNode = {
	canvas: CanvasOption;
	children: CanvasTreeNode[];
};

/**
 * A canvas tree plus the path from its root down to the canvas that was asked
 * about, root first and that canvas last.
 */
export type CanvasTrail = {
	root: CanvasTreeNode;
	path: CanvasOption[];
};
