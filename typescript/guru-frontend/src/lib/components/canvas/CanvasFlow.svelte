<script lang="ts">
import {
	Background,
	MiniMap,
	Panel,
	SvelteFlow,
	useSvelteFlow,
	useUpdateNodeInternals,
	type Connection,
	type Edge
} from '@xyflow/svelte';
import '@xyflow/svelte/dist/style.css';
import { mode } from 'mode-watcher';
import { tick, untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import {
	connectNodePorts,
	createServerNode,
	createStandaloneNode,
	getCanvasGraph,
	locateNodeCanvas
} from '#lib/components/canvas/commands.js';
import { runDeletes, runMoves } from '#lib/components/canvas/selection.js';
import { ZOOM_MAX, ZOOM_MIN, canvasViewport } from '#lib/components/canvas/viewport.svelte.js';
import { setEdgeOpener } from '#lib/components/canvas/edges/open.svelte.js';
import { setFocusedNode } from '#lib/components/canvas/focus.svelte.js';
import {
	buildBackendIndex,
	canvasNames,
	connectEnd,
	buildFlowEdges,
	buildFlowNodes,
	buildPortIndex,
	canConnect,
	flowNodeId,
	keepEdges,
	keepNodes,
	mergeTombstones,
	parseFlowNodeId,
	reconcileFlowEdges,
	reconcileFlowNodes,
	type FlowNode,
	type ForceTarget,
	type PortIndexEntry,
	type PanelTarget,
	type Tombstones
} from '#lib/components/canvas/graph.js';
import BundleEdge from '#lib/components/canvas/edges/BundleEdge.svelte';
import CanvasMenubar from '#lib/components/canvas/CanvasMenubar.svelte';
import CanvasProblems from '#lib/components/canvas/CanvasProblems.svelte';
import CanvasExportNode from '#lib/components/canvas/nodes/CanvasExportNode.svelte';
import CanvasImportNode from '#lib/components/canvas/nodes/CanvasImportNode.svelte';
import EntryNode from '#lib/components/canvas/nodes/EntryNode.svelte';
import ExitNode from '#lib/components/canvas/nodes/ExitNode.svelte';
import LoadBalanceNode from '#lib/components/canvas/nodes/LoadBalanceNode.svelte';
import RelayNode from '#lib/components/canvas/nodes/RelayNode.svelte';
import ServerNode from '#lib/components/canvas/nodes/ServerNode.svelte';
import AddExportDialog from '#lib/components/canvas/panels/AddExportDialog.svelte';
import AddSubcanvasDialog from '#lib/components/canvas/panels/AddSubcanvasDialog.svelte';
import ForceDeleteDialog from '#lib/components/canvas/panels/ForceDeleteDialog.svelte';
import NodePanel from '#lib/components/canvas/panels/NodePanel.svelte';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Resizable from '#lib/components/ui/resizable/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { suggestName } from '#lib/i18n/naming.js';
import { m } from '#lib/paraglide/messages.js';
import { getLocale } from '#lib/paraglide/runtime.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { goto, replaceState } from '$app/navigation';
import { page } from '$app/state';

let { canvasId, editable, admin }: { canvasId: string; editable: boolean; admin: boolean } =
	$props();

const graph = $derived(getCanvasGraph({ canvasId }));

let nodes = $state.raw<FlowNode[]>([]);
let edges = $state.raw<Edge[]>([]);
let portIndex = $state.raw(new Map<string, PortIndexEntry>());
let panelTarget = $state<PanelTarget | null>(null);
let forceTargets = $state<ForceTarget[]>([]);
let flowEl = $state<HTMLDivElement | null>(null);
const view = canvasViewport(() => flowEl);
/**
 * The delete batches still in flight, each owning what it dropped from the
 * mirror. Every delete command refreshes the shared graph query, so without this
 * the first one would pop the items whose command has not run yet back onto the
 * canvas. A batch removes only its own entry when it settles, so two overlapping
 * gestures cannot lift each other's protection; the re-reconcile that follows is
 * what restores anything the control plane refused.
 */
let deleteBatches = $state.raw<Tombstones[]>([]);
const pendingDeletes = $derived(mergeTombstones(deleteBatches));

const { updateNode } = useSvelteFlow();
const updateNodeInternals = useUpdateNodeInternals();

const nodeTypes = {
	server: ServerNode,
	entry: EntryNode,
	relay: RelayNode,
	exit: ExitNode,
	loadBalance: LoadBalanceNode,
	canvasImport: CanvasImportNode,
	canvasExport: CanvasExportNode
};

const edgeTypes = { bundle: BundleEdge };

// The node cards read this to ring the one the panel is editing; an edge in
// the panel rings nothing, Svelte Flow's own selection marks it.
setFocusedNode({
	get current() {
		return panelTarget && panelTarget.kind !== 'edge'
			? flowNodeId(panelTarget.kind, panelTarget.id)
			: null;
	}
});

// A bundle's count pill opens its edge; the edge path itself goes through
// `onedgeclick` below.
setEdgeOpener({ open: id => (panelTarget = { kind: 'edge', id }) });

// The server owns the graph, but a refresh is merged into the local mirror
// instead of replacing it: untouched nodes keep their object identity, so they
// neither re-render nor lose their measured geometry. Only the nodes that
// actually changed are re-measured — handle counts move with pods and
// load-balance members, and stale handle geometry would misplace their edges.
$effect(() => {
	const current = graph.current;
	if (!current) return;
	const gone = pendingDeletes;
	const nextNodes = gone ? keepNodes(buildFlowNodes(current), gone) : buildFlowNodes(current);
	const nextEdges = gone ? keepEdges(buildFlowEdges(current), gone) : buildFlowEdges(current);
	const merged = untrack(() => reconcileFlowNodes(nodes, nextNodes));
	nodes = merged.nodes;
	edges = untrack(() => reconcileFlowEdges(edges, nextEdges));
	portIndex = buildPortIndex(current);
	if (merged.remeasure.length > 0) {
		const ids = merged.remeasure;
		tick().then(() => updateNodeInternals(ids));
	}
});

const refresh = () => getCanvasGraph({ canvasId }).refresh();

function reportError(err: unknown) {
	const body = (err as { body?: App.Error }).body;
	toast.error(errorMessage(body?.code, body?.message ?? ''));
}

const failureMessage = (err: unknown): string => {
	const body = (err as { body?: App.Error }).body;
	return errorMessage(body?.code, body?.message ?? '');
};

const usedNames = () => canvasNames(graph.current);

async function addServer() {
	const { x, y } = view.position();
	try {
		await createServerNode({
			canvasId,
			name: suggestName(m.editor_add_server(), getLocale(), usedNames()),
			x,
			y
		});
	} catch (err) {
		reportError(err);
	}
}

/**
 * The default name is the localized type label plus a random word pair, drawn
 * clear of the names already on the canvas.
 */
async function addNode(
	kind: 'entry' | 'relay' | 'exit' | 'load_balance_distribute' | 'load_balance_aggregate',
	typeLabel: string
) {
	const { x, y } = view.position();
	try {
		await createStandaloneNode({
			canvasId,
			kind,
			name: suggestName(typeLabel, getLocale(), usedNames()),
			x,
			y
		});
	} catch (err) {
		reportError(err);
	}
}

/**
 * The subcanvas dialogs take their placement and default name as callbacks so
 * both are computed when the operator confirms, not when the dialog mounted.
 */
let subcanvasMode = $state<'create' | 'import'>('create');
let subcanvasOpen = $state(false);
let exportDialogOpen = $state(false);
const suggestSubcanvasName = () => suggestName(m.editor_kind_subcanvas(), getLocale(), usedNames());
const suggestExportName = () => suggestName(m.editor_kind_export(), getLocale(), usedNames());

/**
 * Descends into the canvas an import node embeds. Svelte Flow has no
 * double-click event and delivers a pointer event whose `detail` is always 0,
 * so the second click is recognised here: same node, within the usual
 * double-click window.
 */
const DOUBLE_CLICK_MS = 400;
let lastClick: { id: string; at: number } | null = null;

function selectNode(node: FlowNode) {
	panelTarget = parseFlowNodeId(node.id);
	const now = Date.now();
	const again = lastClick?.id === node.id && now - lastClick.at < DOUBLE_CLICK_MS;
	lastClick = again ? null : { id: node.id, at: now };
	if (!again || node.data.kind !== 'canvas_import') return;
	const target = node.data.node.targetCanvasId;
	if (target) goto(`/canvas/${target}`);
}

async function persistMove(dragged: FlowNode[]) {
	try {
		await runMoves(canvasId, dragged);
	} catch (err) {
		reportError(err);
		await refresh();
	}
}

const ARROW_KEYS: Record<string, true> = {
	ArrowUp: true,
	ArrowDown: true,
	ArrowLeft: true,
	ArrowRight: true
};

/**
 * Svelte Flow nudges the selection on arrow keys but never fires a drag stop, so
 * the release is what persists the new positions. Only key events aimed at the
 * flow count: the node panel's inputs handle their own arrows.
 */
function nudgeStop(event: KeyboardEvent) {
	if (!editable || !view.interactive || !ARROW_KEYS[event.key]) return;
	if (!(event.target instanceof Node) || !flowEl?.contains(event.target)) return;
	const selected = nodes.filter(node => node.selected);
	if (selected.length > 0) persistMove(selected);
}

const isValidConnection = (connection: Edge | Connection): boolean => {
	const current = graph.current;
	return current ? canConnect(connection, portIndex, current, edges) : false;
};

async function connect(connection: Connection) {
	const current = graph.current;
	if (!current) return;
	try {
		await connectNodePorts({
			canvasId,
			output: connectEnd(current, connection.sourceHandle ?? ''),
			input: connectEnd(current, connection.targetHandle ?? '')
		});
		toast.success(m.editor_connected());
	} catch (err) {
		reportError(err);
		// Drops the optimistic edge Svelte Flow inserted.
		await refresh();
	}
}

/**
 * Always returns `false`: deletions are the server's to make. The whole doomed
 * selection leaves the local mirror before any command is awaited, so the canvas
 * reacts to the keypress immediately instead of trailing the slowest call, and
 * stays gone for the rest of the batch through `pendingDeletes`. Clearing that
 * re-reconciles against the graph the control plane actually kept, which is what
 * puts a refused item back.
 */
async function beforeDelete({
	nodes: doomedNodes,
	edges: doomedEdges
}: {
	nodes: FlowNode[];
	edges: Edge[];
}): Promise<boolean> {
	const gone: Tombstones = {
		nodes: new Set(doomedNodes.map(node => node.id)),
		edges: new Set(doomedEdges.map(edge => edge.id))
	};
	deleteBatches = [...deleteBatches, gone];
	nodes = keepNodes(nodes, gone);
	edges = keepEdges(edges, gone);

	const outcome = await runDeletes(
		canvasId,
		{ nodes: doomedNodes, edges: doomedEdges },
		failureMessage
	).finally(() => {
		// This batch has settled: it stops hiding its own items, so the reconcile
		// that follows brings back whatever the control plane refused. Any batch
		// still running keeps hiding its own.
		deleteBatches = deleteBatches.filter(batch => batch !== gone);
	});

	if (outcome.failures.length > 0) {
		// Forcing needs admin; everyone else only gets the reason.
		if (admin) forceTargets = outcome.failures;
		else toast.error(outcome.failures[0]?.message ?? '');
		// Every refusal above left the server untouched, but a call can also fail
		// after the control plane acted, so this one case is re-read.
		await refresh();
	} else if (outcome.deleted) {
		toast.success(m.editor_deleted());
	}

	return false;
}

/** Selects a node of this canvas and opens its panel. */
function focusNode(current: CanvasGraph, nodeId: string): boolean {
	const located = buildBackendIndex(current).get(nodeId);
	if (!located) return false;
	updateNode(located.flowId, { selected: true });
	panelTarget = located.target;
	return true;
}

/**
 * Jumps to the node a problem concerns. Validation runs on the whole canvas
 * tree, so the node may live in another canvas of it: then the editor opens
 * that canvas and hands the node over in `?focus`, which the instance mounted
 * there consumes below.
 */
async function openProblem(nodeIds: string[]) {
	const current = graph.current;
	const first = nodeIds[0];
	if (!current || first === undefined) return;
	if (focusNode(current, first)) return;
	try {
		const owner = await locateNodeCanvas({ canvasId, nodeId: first });
		if (owner) await goto(`/canvas/${owner}?focus=${encodeURIComponent(first)}`);
	} catch (err) {
		reportError(err);
	}
}

/**
 * The handover from a problem opened on another canvas of the tree. It runs
 * once the graph is there, then drops the parameter so a reload does not
 * re-select a node the operator has since moved on from.
 */
let focusHandled = $state('');
$effect(() => {
	const wanted = page.url.searchParams.get('focus');
	// No parameter: the last handover is spent, so the same node may be handed
	// over again later — this component instance survives the navigation.
	if (!wanted) {
		focusHandled = '';
		return;
	}
	const current = graph.current;
	if (!current || focusHandled === wanted) return;
	focusHandled = wanted;
	if (!untrack(() => focusNode(current, wanted))) return;
	const url = new URL(page.url.href);
	url.searchParams.delete('focus');
	replaceState(url, page.state);
});
</script>

<svelte:window onkeyup={nudgeStop} />
<svelte:document
	onfullscreenchange={() => (view.fullscreen = document.fullscreenElement !== null)}
/>

<svelte:boundary>
	<!-- Only the first load has nothing to show: a refresh keeps the flow mounted,
	     otherwise remounting it would re-run `fitView` and reset the viewport. -->
	{#if graph.current === undefined}
		<Skeleton class="h-full w-full" />
	{:else}
		{@const current = graph.current}
		<Resizable.PaneGroup direction="horizontal">
			<Resizable.Pane defaultSize={70} minSize={40} order={1}>
				<div class="relative h-full w-full" bind:this={flowEl}>
					<SvelteFlow
						bind:nodes
						bind:edges
						{nodeTypes}
						{edgeTypes}
						fitView
						minZoom={ZOOM_MIN}
						maxZoom={ZOOM_MAX}
						colorMode={mode.current ?? 'system'}
						nodesDraggable={editable && view.interactive}
						nodesConnectable={editable && view.interactive}
						elementsSelectable={view.interactive}
						{isValidConnection}
						onconnect={connect}
						onbeforedelete={beforeDelete}
						onnodeclick={({ node }) => selectNode(node)}
						onedgeclick={({ edge }) => (panelTarget = { kind: 'edge', id: edge.id })}
						onnodedragstop={({ nodes: dragged }) => persistMove(dragged)}
						onmove={(_, viewport) => (view.zoom = viewport.zoom)}
						deleteKey={editable && view.interactive ? 'Delete' : null}
					>
						<Background />
						<MiniMap />

						<Panel position="top-left">
							<CanvasMenubar
								{editable}
								{view}
								onAddServer={addServer}
								onAddNode={addNode}
								onSubcanvas={mode => {
									subcanvasMode = mode;
									subcanvasOpen = true;
								}}
								onExport={() => (exportDialogOpen = true)}
							/>
						</Panel>

						<Panel position="bottom-left">
							<CanvasProblems graph={current} onopen={openProblem} />
						</Panel>
					</SvelteFlow>

					{#if current && current.servers.length === 0 && current.nodes.length === 0}
						<div class="pointer-events-none absolute inset-0 flex items-center justify-center">
							<Empty.Root>
								<Empty.Header>
									<Empty.Title>{m.editor_empty_title()}</Empty.Title>
									<Empty.Description>{m.editor_empty_description()}</Empty.Description>
								</Empty.Header>
							</Empty.Root>
						</div>
					{/if}
				</div>
			</Resizable.Pane>

			<!-- Conditional panes need `order` so the flow stays first when this mounts. -->
			{#if panelTarget}
				<Resizable.Handle withHandle />
				<Resizable.Pane defaultSize={30} minSize={20} maxSize={50} order={2}>
					<NodePanel bind:target={panelTarget} {canvasId} {editable} {admin} graph={current} />
				</Resizable.Pane>
			{/if}
		</Resizable.PaneGroup>

		{#if admin}
			<ForceDeleteDialog bind:targets={forceTargets} {canvasId} />
		{/if}

		{#if editable}
			<AddSubcanvasDialog
				bind:open={subcanvasOpen}
				mode={subcanvasMode}
				{canvasId}
				place={view.position}
				suggest={suggestSubcanvasName}
			/>
			<AddExportDialog
				bind:open={exportDialogOpen}
				{canvasId}
				place={view.position}
				suggest={suggestExportName}
			/>
		{/if}
	{/if}

	{#snippet failed(error)}
		{@const body = (error as { body?: App.Error }).body}
		<Empty.Root>
			<Empty.Header>
				<Empty.Title>{m.error_title()}</Empty.Title>
				<Empty.Description>{errorMessage(body?.code, body?.message ?? '')}</Empty.Description>
			</Empty.Header>
		</Empty.Root>
	{/snippet}
</svelte:boundary>
