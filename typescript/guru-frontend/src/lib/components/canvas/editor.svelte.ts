import type { Drawing, EditErrorCode, GraphChange, Id, Target } from 'guru-graph';
import { getContext, setContext } from 'svelte';
import type { CanvasGraph, DiagnosticDto, ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import type { PanelTarget } from './flow/nodes.js';

/**
 * A change shown to the operator before it is applied: what it takes away, and
 * what the control plane says about the graph it leads to.
 */
export type ReviewRequest = {
	title: string;
	/** A sentence about what the gesture means, above the counts. */
	description: string;
	/**
	 * Whether "also remove relay pods left with no way in" is offered; `build`
	 * is called again whenever it is toggled.
	 */
	prunable: boolean;
	build(prune: boolean): GraphChange;
	/** What has to happen once the batch is written: deleting servers, canvases. */
	after?: () => Promise<unknown>;
	/** Servers and subcanvases `after` deletes, for the summary. */
	alsoDeletes?: { servers: string[]; canvases: string[] };
	success: string;
};

/** Where a new way on may lead, as the target dialog offers it. */
export type PickRequest = {
	title: string;
	/** Only pods, exits and servers of this canvas and the canvases inside it. */
	scope: Id;
	/** A server whose new relay pod is the only choice: only the protocol is asked. */
	server?: Id;
	/** Pods that may not be chosen (the pod the way starts at). */
	excludePods?: Id[];
};

/** What panels and dialogs do to the graph, through the canvas that owns it. */
export type Editor = {
	readonly canvasId: Id;
	readonly graph: CanvasGraph;
	readonly drawing: Drawing<ServerDto>;
	readonly editable: boolean;
	readonly admin: boolean;
	/**
	 * Applies one batch against the graph as it was read, keeping the drawing's
	 * layout. Resolves `true` once it is written; a refusal is reported and
	 * resolves `false`. A thrown `EditError` is reported the same way.
	 */
	commit(change: GraphChange | (() => GraphChange), success?: string): Promise<boolean>;
	/** Shows a change with its dry-run diagnostics; applies it when confirmed. */
	review(request: ReviewRequest): void;
	/** Asks where a new way on should lead; `null` when the operator gave up. */
	pickTarget(request: PickRequest): Promise<Target | null>;
	open(target: PanelTarget): void;
};

const KEY = Symbol.for('guru-canvas-editor');

export function setEditor(editor: Editor): void {
	setContext(KEY, editor);
}

export function useEditor(): Editor {
	const editor = getContext<Editor | undefined>(KEY);
	if (!editor) throw new Error('useEditor outside a canvas');
	return editor;
}

/** Why a gesture could not mean anything, in the operator's language. */
export function editErrorText(code: EditErrorCode): string {
	switch (code) {
		case 'pod_not_found':
			return m.editor_edit_pod_not_found();
		case 'exit_not_found':
			return m.editor_edit_exit_not_found();
		case 'server_not_found':
			return m.editor_edit_server_not_found();
		case 'splitter_not_found':
			return m.editor_edit_splitter_not_found();
		case 'aggregator_not_found':
			return m.editor_edit_aggregator_not_found();
		case 'target_not_dialable':
		case 'client_pod_dialed':
			return m.editor_edit_client_pod_dialed();
		case 'self_dial':
			return m.editor_edit_self_dial();
		case 'cycle':
			return m.editor_edit_cycle();
	}
}

/** The errors of a refused batch, the way a toast says them. */
export function refusalText(diagnostics: DiagnosticDto[]): string {
	const errors = diagnostics.filter(diagnostic => diagnostic.error);
	const [first] = errors;
	if (!first) return m.editor_refused_unknown();
	return errors.length === 1
		? first.message
		: m.editor_refused_more({ message: first.message, count: errors.length - 1 });
}

/** How much a batch takes away and adds, for the review dialog. */
export function changeSummary(change: GraphChange) {
	return {
		podsRemoved: change.deletePodIds.length,
		edgesRemoved: change.deleteEdgeIds.length,
		exitsRemoved: change.deleteExitIds.length,
		podsWritten: change.putPods.length,
		edgesWritten: change.putEdges.length
	};
}
