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
	deleteNode,
	deleteServerNode,
	disconnectEdge,
	getCanvasGraph,
	locateNodeCanvas,
	moveNode,
	moveServerNode
} from '#lib/components/canvas/commands.js';
import { setFocusedNode } from '#lib/components/canvas/focus.svelte.js';
import {
	buildBackendIndex,
	buildFlowEdges,
	buildFlowNodes,
	buildPortIndex,
	canConnect,
	flowNodeId,
	keepEdges,
	keepNodes,
	mergeTombstones,
	parseFlowNodeId,
	parseGroupHandle,
	reconcileFlowEdges,
	reconcileFlowNodes,
	type FlowNode,
	type ForceTarget,
	type PortIndexEntry,
	type PanelTarget,
	type Tombstones
} from '#lib/components/canvas/graph.js';
import BundleEdge from '#lib/components/canvas/edges/BundleEdge.svelte';
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
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Menubar from '#lib/components/ui/menubar/index.js';
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
let problemsOpen = $state(false);
let flowEl = $state<HTMLDivElement | null>(null);
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

const {
	screenToFlowPosition,
	updateNode,
	getViewport,
	setViewport,
	getZoom,
	fitView: fitViewport
} = useSvelteFlow();
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

// The node cards read this to ring the one the panel is editing.
setFocusedNode({
	get current() {
		return panelTarget ? flowNodeId(panelTarget.kind, panelTarget.id) : null;
	}
});

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

/** Centre of the visible pane, in flow coordinates. */
function palettePosition(): { x: number; y: number } {
	const rect = flowEl?.getBoundingClientRect();
	if (!rect) return { x: 0, y: 0 };
	const point = screenToFlowPosition({
		x: rect.left + rect.width / 2,
		y: rect.top + rect.height / 2
	});
	return { x: Math.round(point.x), y: Math.round(point.y) };
}

/**
 * The View menu stands in for Svelte Flow's `Controls`: `interactive` is the
 * lock button (dragging, connecting and selecting at once), and the zoom label
 * follows the viewport.
 */
let interactive = $state(true);
let fullscreen = $state(false);
let zoom = $state(1);

/**
 * Fullscreens the document, not the flow pane: the menus and dialogs portal into
 * `document.body`, so anything smaller would render them outside the fullscreen
 * element and make them invisible.
 */
function toggleFullscreen() {
	const request = document.fullscreenElement
		? document.exitFullscreen()
		: document.documentElement.requestFullscreen();
	// A refused request is the browser's call, not a failure worth reporting.
	request.catch(() => undefined);
}

const ZOOM_MIN = 0.2;
const ZOOM_MAX = 2;
const ZOOM_STEP = 1.2;

/**
 * Steps the zoom around the centre of the pane, or fits the whole canvas. The
 * viewport is moved with `setViewport` rather than `zoomIn`/`zoomOut`, whose
 * `scaleBy` leaves the transform untouched in Svelte Flow 1.6.
 */
async function rescale(step: 'in' | 'out' | 'fit') {
	if (step === 'fit') {
		await fitViewport();
	} else {
		const current = getViewport();
		const wanted = current.zoom * (step === 'in' ? ZOOM_STEP : 1 / ZOOM_STEP);
		const next = Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, wanted));
		const rect = flowEl?.getBoundingClientRect();
		// The pane centre stays put: it is the point the zoom is anchored on.
		const cx = (rect?.width ?? 0) / 2;
		const cy = (rect?.height ?? 0) / 2;
		const ratio = next / current.zoom;
		await setViewport({
			x: cx - (cx - current.x) * ratio,
			y: cy - (cy - current.y) * ratio,
			zoom: next
		});
	}
	zoom = getZoom();
}

/**
 * Every name visible on the canvas, so a fresh default never duplicates one.
 * Pods are included: they render inside their server and carry a name of their
 * own, so a node named after one would be just as confusing.
 */
function usedNames(): ReadonlySet<string> {
	const current = graph.current;
	if (!current) return new Set();
	const names = new Set<string>();
	for (const server of current.servers) {
		names.add(server.name);
		for (const pod of server.pods) names.add(pod.name);
	}
	for (const node of current.nodes) names.add(node.name);
	for (const pod of current.orphanPods) names.add(pod.name);
	return names;
}

async function addServer() {
	const { x, y } = palettePosition();
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
	const { x, y } = palettePosition();
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

/**
 * Positions are persisted on drop. These two commands deliberately skip the
 * query refresh: the local position already matches, so re-rendering the whole
 * graph after every drag would only cost a flicker.
 */
async function persistMove(dragged: FlowNode[]) {
	try {
		for (const node of dragged) {
			const { kind, id } = parseFlowNodeId(node.id);
			const x = Math.round(node.position.x);
			const y = Math.round(node.position.y);
			if (kind === 'server') {
				await moveServerNode({ canvasId, serverId: id, x, y });
			} else if (node.data.kind !== 'server') {
				// `UpdateNodeMeta` replaces name and comment, so they are resent as-is.
				// An export's y orders the mirrored ports on the parent's import node.
				await moveNode({
					canvasId,
					nodeId: id,
					name: node.data.node.name,
					comment: node.data.node.comment,
					x,
					y,
					boundary: node.data.kind === 'canvas_export'
				});
			}
		}
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
	if (!editable || !interactive || !ARROW_KEYS[event.key]) return;
	if (!(event.target instanceof Node) || !flowEl?.contains(event.target)) return;
	const selected = nodes.filter(node => node.selected);
	if (selected.length > 0) persistMove(selected);
}

const isValidConnection = (connection: Edge | Connection): boolean => {
	const current = graph.current;
	return current ? canConnect(connection, portIndex, current, edges) : false;
};

/**
 * A handle is either a port id or a bundle-capable node's group (`u:<flow>:<group>`);
 * the control plane creates the port behind a group in the same write.
 */
function connectEnd(
	handle: string
): { portId: string } | { nodeId: string; group: 'channel_out' | 'bundle_in' } {
	const group = parseGroupHandle(handle);
	if (!group) return { portId: handle };
	const { kind, id } = parseFlowNodeId(group.flowId);
	// A server card's bundle handles belong to the universal pod drawn inside it.
	const nodeId =
		kind === 'server'
			? (graph.current?.servers.find(server => server.id === id)?.universal?.nodeId ?? '')
			: id;
	return { nodeId, group: group.group };
}

async function connect(connection: Connection) {
	try {
		await connectNodePorts({
			canvasId,
			output: connectEnd(connection.sourceHandle ?? ''),
			input: connectEnd(connection.targetHandle ?? '')
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
	const failures: ForceTarget[] = [];
	let deleted = false;

	const gone: Tombstones = {
		nodes: new Set(doomedNodes.map(node => node.id)),
		edges: new Set(doomedEdges.map(edge => edge.id))
	};
	deleteBatches = [...deleteBatches, gone];
	nodes = keepNodes(nodes, gone);
	edges = keepEdges(edges, gone);

	try {
		for (const edge of doomedEdges) {
			try {
				await disconnectEdge({ canvasId, edgeId: edge.id, force: false });
				deleted = true;
			} catch (err) {
				failures.push({
					kind: 'edge',
					id: edge.id,
					label: m.editor_disconnected(),
					message: failureMessage(err)
				});
			}
		}

		for (const node of doomedNodes) {
			const { kind, id } = parseFlowNodeId(node.id);
			const label = node.data.kind === 'server' ? node.data.server.name : node.data.node.name;
			// Retiring an import hands its target back to the root listing; retiring
			// an export takes its mirrored port off the parent's import node.
			const subcanvasTarget =
				node.data.kind === 'canvas_import' ? node.data.node.targetCanvasId : '';
			const boundary = node.data.kind === 'canvas_export';
			try {
				if (kind === 'server') await deleteServerNode({ canvasId, serverId: id, force: false });
				else await deleteNode({ canvasId, nodeId: id, force: false, subcanvasTarget, boundary });
				deleted = true;
			} catch (err) {
				failures.push({
					kind,
					id,
					label,
					message: failureMessage(err),
					subcanvasTarget,
					boundary
				});
			}
		}
	} finally {
		// This batch has settled: it stops hiding its own items, so the reconcile
		// that follows brings back whatever the control plane refused. Any batch
		// still running keeps hiding its own.
		deleteBatches = deleteBatches.filter(batch => batch !== gone);
	}

	if (failures.length > 0) {
		// Forcing needs admin; everyone else only gets the reason.
		if (admin) forceTargets = failures;
		else toast.error(failures[0]?.message ?? '');
		// Every refusal above left the server untouched, but a call can also fail
		// after the control plane acted, so this one case is re-read.
		await refresh();
	} else if (deleted) {
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
	onfullscreenchange={() => (fullscreen = document.fullscreenElement !== null)}
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
						nodesDraggable={editable && interactive}
						nodesConnectable={editable && interactive}
						elementsSelectable={interactive}
						{isValidConnection}
						onconnect={connect}
						onbeforedelete={beforeDelete}
						onnodeclick={({ node }) => selectNode(node)}
						onnodedragstop={({ nodes: dragged }) => persistMove(dragged)}
						onmove={(_, viewport) => (zoom = viewport.zoom)}
						deleteKey={editable && interactive ? 'Delete' : null}
					>
						<Background />
						<MiniMap />

						<Panel position="top-left">
							<!-- The bar floats over the grid, so it needs a surface of its own. -->
							<Menubar.Root class="bg-background shadow-sm">
								{#if editable}
									<Menubar.Menu>
										<Menubar.Trigger>{m.editor_menu_nodes()}</Menubar.Trigger>
										<Menubar.Content>
											<Menubar.Group>
												<Menubar.Item onSelect={addServer}>{m.editor_new_server()}</Menubar.Item>
												<Menubar.Item onSelect={() => addNode('entry', m.editor_add_entry())}>
													{m.editor_new_entry()}
												</Menubar.Item>
												<Menubar.Item onSelect={() => addNode('relay', m.editor_add_relay())}>
													{m.editor_new_relay()}
												</Menubar.Item>
												<Menubar.Item onSelect={() => addNode('exit', m.editor_add_exit())}>
													{m.editor_new_exit()}
												</Menubar.Item>
											</Menubar.Group>
											<Menubar.Separator />
											<Menubar.Group>
												<Menubar.Item
													onSelect={() =>
														addNode('load_balance_distribute', m.editor_add_lb_distribute())}
												>
													{m.editor_new_lb_distribute()}
												</Menubar.Item>
												<Menubar.Item
													onSelect={() =>
														addNode('load_balance_aggregate', m.editor_add_lb_aggregate())}
												>
													{m.editor_new_lb_aggregate()}
												</Menubar.Item>
											</Menubar.Group>
										</Menubar.Content>
									</Menubar.Menu>

									<Menubar.Menu>
										<Menubar.Trigger>{m.editor_kind_subcanvas()}</Menubar.Trigger>
										<Menubar.Content>
											<Menubar.Group>
												<Menubar.Item
													onSelect={() => {
														subcanvasMode = 'create';
														subcanvasOpen = true;
													}}
												>
													{m.editor_subcanvas_create_title()}
												</Menubar.Item>
												<Menubar.Item
													onSelect={() => {
														subcanvasMode = 'import';
														subcanvasOpen = true;
													}}
												>
													{m.editor_menu_subcanvas_import()}
												</Menubar.Item>
												<Menubar.Item onSelect={() => (exportDialogOpen = true)}>
													{m.editor_kind_export()}
												</Menubar.Item>
											</Menubar.Group>
										</Menubar.Content>
									</Menubar.Menu>
								{/if}

								<!-- Opening the menu re-reads the zoom: `fitView` and the node panel move
								     the viewport without a move event. -->
								<Menubar.Menu
									onOpenChange={open => {
										if (open) zoom = getZoom();
									}}
								>
									<Menubar.Trigger>{m.editor_menu_view()}</Menubar.Trigger>
									<Menubar.Content>
										<Menubar.Group>
											<Menubar.CheckboxItem
												checked={fullscreen}
												onCheckedChange={toggleFullscreen}
											>
												{m.editor_view_fullscreen()}
											</Menubar.CheckboxItem>
											<Menubar.CheckboxItem bind:checked={interactive}>
												{m.editor_view_interactive()}
											</Menubar.CheckboxItem>
										</Menubar.Group>
										<Menubar.Separator />
										<Menubar.Group>
											<Menubar.GroupHeading>
												{m.editor_view_zoom({ percent: Math.round(zoom * 100) })}
											</Menubar.GroupHeading>
											<Menubar.Item closeOnSelect={false} onSelect={() => rescale('in')}>
												{m.editor_view_zoom_in()}
											</Menubar.Item>
											<Menubar.Item closeOnSelect={false} onSelect={() => rescale('out')}>
												{m.editor_view_zoom_out()}
											</Menubar.Item>
											<Menubar.Item onSelect={() => rescale('fit')}>
												{m.editor_view_zoom_fit()}
											</Menubar.Item>
										</Menubar.Group>
									</Menubar.Content>
								</Menubar.Menu>
							</Menubar.Root>
						</Panel>

						<Panel position="bottom-left">
							{#if current}
								{@const problems = current.problems}
								{#if problems.length === 0}
									<Badge variant="secondary">{m.canvas_health_ok()}</Badge>
								{:else if problems.length > 3 && !problemsOpen}
									<Button size="sm" variant="outline" onclick={() => (problemsOpen = true)}>
										{m.editor_problems({ count: problems.length })}
									</Button>
								{:else}
									<Card.Root class="max-w-md gap-2 py-3">
										<Card.Content class="grid gap-2 px-3">
											{#if current.orphanPods.length > 0}
												<p class="text-xs text-muted-foreground">
													{m.editor_pod_orphan({ count: current.orphanPods.length })}
												</p>
											{/if}
											{#each problems as problem (problem.message)}
												<button
													type="button"
													class="flex items-start gap-2 text-start text-xs hover:underline"
													onclick={() => openProblem(problem.nodeIds)}
												>
													<Badge
														variant={problem.severity === 'error' ? 'destructive' : 'outline'}
													>
														{problem.severity === 'error'
															? m.editor_severity_error()
															: m.editor_severity_warning()}
													</Badge>
													<span>{problem.message}</span>
												</button>
											{/each}
										</Card.Content>
									</Card.Root>
								{/if}
							{/if}
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
				place={palettePosition}
				suggest={suggestSubcanvasName}
			/>
			<AddExportDialog
				bind:open={exportDialogOpen}
				{canvasId}
				place={palettePosition}
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
