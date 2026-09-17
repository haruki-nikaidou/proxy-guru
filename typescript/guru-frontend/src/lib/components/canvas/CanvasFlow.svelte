<script lang="ts">
import {
	Background,
	type Connection,
	type Edge,
	MiniMap,
	Panel,
	SvelteFlow,
	useSvelteFlow,
	useUpdateNodeInternals
} from '@xyflow/svelte';
import '@xyflow/svelte/dist/style.css';
import {
	addSplitterMember,
	addWayOn,
	canDropWayOn,
	carryLayout,
	clearSpot,
	connect,
	connectEach,
	draw,
	EditError,
	fanOutSiblings,
	type GraphChange,
	type Handle,
	isEmptyChange,
	isRelay,
	joinSplitter,
	locate,
	moveCards,
	removeAll,
	routeNodesAt,
	type Subject,
	type Target,
	targetOf
} from 'guru-graph';
import { mode } from 'mode-watcher';
import { tick, untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { goto, replaceState } from '$app/navigation';
import { page } from '$app/state';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import LiveBadge from '#lib/components/LiveBadge.svelte';
import CanvasMenubar from '#lib/components/canvas/CanvasMenubar.svelte';
import CanvasProblems from '#lib/components/canvas/CanvasProblems.svelte';
import {
	applyGraphChange,
	createServerNode,
	deleteServerNode,
	deleteSubcanvas,
	moveCanvasItems,
	watchCanvasGraph
} from '#lib/components/canvas/commands.js';
import { paletteColor, setCanvasContext } from '#lib/components/canvas/context.svelte.js';
import AddExitDialog from '#lib/components/canvas/dialogs/AddExitDialog.svelte';
import AddSubcanvasDialog from '#lib/components/canvas/dialogs/AddSubcanvasDialog.svelte';
import ReviewDialog from '#lib/components/canvas/dialogs/ReviewDialog.svelte';
import TargetDialog from '#lib/components/canvas/dialogs/TargetDialog.svelte';
import {
	editErrorText,
	type PickRequest,
	type ReviewRequest,
	refusalText,
	setEditor
} from '#lib/components/canvas/editor.svelte.js';
import BusEdge from '#lib/components/canvas/edges/BusEdge.svelte';
import { buildFlowEdges } from '#lib/components/canvas/flow/edges.js';
import {
	buildFlowNodes,
	type FlowNode,
	focusedCard,
	type PanelTarget,
	problemIndex,
	usedNames
} from '#lib/components/canvas/flow/nodes.js';
import { reconcileFlowEdges, reconcileFlowNodes } from '#lib/components/canvas/flow/reconcile.js';
import RuleLegend from '#lib/components/canvas/RuleLegend.svelte';
import AggregatorCard from '#lib/components/canvas/nodes/AggregatorCard.svelte';
import ExitCard from '#lib/components/canvas/nodes/ExitCard.svelte';
import PortalCard from '#lib/components/canvas/nodes/PortalCard.svelte';
import ServerCard from '#lib/components/canvas/nodes/ServerCard.svelte';
import SplitterCard from '#lib/components/canvas/nodes/SplitterCard.svelte';
import SubcanvasCard from '#lib/components/canvas/nodes/SubcanvasCard.svelte';
import InspectorPanel from '#lib/components/canvas/panels/InspectorPanel.svelte';
import { canvasViewport, ZOOM_MAX, ZOOM_MIN } from '#lib/components/canvas/viewport.svelte.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Resizable from '#lib/components/ui/resizable/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { suggestName } from '#lib/i18n/naming.js';
import { reconnectWhenTransient } from '#lib/live.svelte.js';
import { m } from '#lib/paraglide/messages.js';
import { getLocale } from '#lib/paraglide/runtime.js';
import { reportError } from '#lib/report.js';

let { canvasId, editable, admin }: { canvasId: string; editable: boolean; admin: boolean } =
	$props();

const query = $derived(watchCanvasGraph({ canvasId }));
reconnectWhenTransient(() => query);
const graph = $derived(query.current);
const drawing = $derived(graph ? draw(graph, canvasId) : undefined);
const problems = $derived(graph && drawing ? problemIndex(graph, drawing) : undefined);

let nodes = $state.raw<FlowNode[]>([]);
let edges = $state.raw<Edge[]>([]);
let panelTarget = $state<PanelTarget | null>(null);
let highlightedRule = $state<string | null>(null);
let flowEl = $state<HTMLDivElement | null>(null);
const view = canvasViewport(() => flowEl);

let reviewRequest = $state<ReviewRequest | null>(null);
let pick = $state<{ request: PickRequest; resolve: (target: Target | null) => void } | null>(null);
let subcanvasOpen = $state(false);
let exitOpen = $state(false);

const { fitView, updateNode } = useSvelteFlow();
const updateNodeInternals = useUpdateNodeInternals();

const nodeTypes = {
	server: ServerCard,
	exit: ExitCard,
	canvas: SubcanvasCard,
	portal: PortalCard,
	splitter: SplitterCard,
	aggregator: AggregatorCard
};
const edgeTypes = { bus: BusEdge };

const podNames = $derived(new Map((graph?.pods ?? []).map(pod => [pod.id, pod.name])));

setCanvasContext({
	get focusedCard() {
		return graph ? focusedCard(graph, panelTarget) : null;
	},
	get focusedPod() {
		return panelTarget?.kind === 'pod' ? panelTarget.id : null;
	},
	get highlightedRule() {
		return highlightedRule;
	},
	open: target => {
		if (target.kind === 'bus') clickBus(target.id);
		else panelTarget = target;
	},
	ruleColor: rule => paletteColor(drawing?.rules.colors.get(rule) ?? 0),
	ruleName: rule => podNames.get(rule) ?? rule
});

// The server owns the graph, but a refresh is merged into the local mirror
// instead of replacing it: untouched cards keep their object identity, so they
// neither re-render nor lose their measured geometry. Only the cards that
// actually changed are re-measured — handles come and go with pods and members,
// and stale handle geometry would misplace their buses.
function redraw() {
	if (!graph || !drawing || !problems) return;
	const merged = untrack(() => reconcileFlowNodes(nodes, buildFlowNodes(graph, drawing, problems)));
	nodes = merged.nodes;
	edges = untrack(() => reconcileFlowEdges(edges, buildFlowEdges(drawing, problems)));
	const measured = merged.remeasure.length > 0 ? merged.remeasure : null;
	// `fitView` on the flow only runs when it mounts; moving to another canvas
	// of the tree keeps it mounted, so the first drawing of each canvas is fitted
	// here, once its cards have been measured.
	const fit = fittedCanvas !== drawing.canvasId;
	fittedCanvas = drawing.canvasId;
	if (!measured && !fit) return;
	tick().then(() => {
		if (measured) updateNodeInternals(measured);
		if (fit)
			requestAnimationFrame(() =>
				fitView({ padding: 0.1, maxZoom: 1 }).then(() => view.syncZoom())
			);
	});
}
let fittedCanvas = '';
$effect(redraw);

// The component stays mounted when the operator moves to another canvas of the
// tree: what the panel and the legend were pointed at belongs to the last one.
let shownCanvas = '';
$effect.pre(() => {
	if (canvasId === shownCanvas) return;
	shownCanvas = canvasId;
	untrack(() => {
		panelTarget = null;
		highlightedRule = null;
	});
});

/** Re-opens the live stream, which answers with a fresh snapshot. */
const refresh = () => watchCanvasGraph({ canvasId }).reconnect();

/** How long a committed edit waits for its own snapshot before the caller moves on. */
const SETTLE_TIMEOUT = 3000;

/**
 * Resolves once the live graph has reached `generation`, or after
 * SETTLE_TIMEOUT. A write's own snapshot follows it by one Redis hop; waiting
 * for it keeps a second edit in quick succession from carrying a stale
 * `expectedGeneration`.
 */
function settled(generation: number): Promise<void> {
	if ((graph?.generation ?? 0) >= generation) return Promise.resolve();
	return new Promise(resolve => {
		const timer = setTimeout(done, SETTLE_TIMEOUT);
		const stop = $effect.root(() => {
			$effect(() => {
				if ((graph?.generation ?? 0) >= generation) done();
			});
		});
		function done() {
			clearTimeout(timer);
			stop();
			resolve();
		}
	});
}

/** Reports what went wrong with a gesture or a write. */
function report(err: unknown) {
	if (err instanceof EditError) toast.error(editErrorText(err.code));
	else reportError(err);
}

/**
 * Applies a batch. `base` is the graph a gesture built it from when that can be
 * older than the graph now — the operator took a while in a dialog: its
 * generation is the one the write expects, so a batch naming routes that have
 * changed since is refused instead of written.
 */
async function commit(
	change: GraphChange | (() => GraphChange),
	success?: string,
	base?: { generation: number }
) {
	const current = graph;
	if (!current) return false;
	try {
		const batch = carryLayout(current, canvasId, typeof change === 'function' ? change() : change);
		if (isEmptyChange(batch)) return true;
		const outcome = await applyGraphChange({
			canvasId,
			change: batch,
			expectedGeneration: (base ?? current).generation
		});
		if (!outcome.applied) {
			toast.error(refusalText(outcome.diagnostics));
			return false;
		}
		if (success) toast.success(success);
		// A groups-only batch leaves the generation where it was and resolves at once.
		await settled(outcome.generation);
		return true;
	} catch (err) {
		report(err);
		// A stale generation, or a write that half happened: re-read either way.
		await refresh();
		return false;
	}
}

function pickTarget(request: PickRequest): Promise<Target | null> {
	return new Promise(resolve => {
		pick?.resolve(null);
		pick = { request, resolve };
	});
}

setEditor({
	get canvasId() {
		return canvasId;
	},
	get graph() {
		return graph as CanvasGraph;
	},
	get drawing() {
		return drawing as NonNullable<typeof drawing>;
	},
	get editable() {
		return editable;
	},
	get admin() {
		return admin;
	},
	commit,
	review: request => {
		reviewRequest = request;
	},
	pickTarget,
	open: target => {
		if (target.kind === 'bus') clickBus(target.id);
		else panelTarget = target;
	}
});

// --- adding ------------------------------------------------------------------

/** Near the middle of the view, clear of every card already drawn. */
const spotFor = (kind: 'server' | 'exit' | 'canvas') => () =>
	drawing ? clearSpot(drawing, view.position(), kind) : view.position();

async function addServer() {
	const { x, y } = spotFor('server')();
	try {
		await createServerNode({
			canvasId,
			name: suggestName(m.editor_add_server(), getLocale(), usedNames(graph)),
			x,
			y
		});
	} catch (err) {
		report(err);
	}
}

// --- connecting --------------------------------------------------------------

/**
 * What a drop on a card means as a target, asking the operator where needed;
 * `exclude` are the pods the drag starts from, which may not be picked.
 */
async function dropTarget(node: string, handle: string, exclude: string[]): Promise<Target | null> {
	if (handle.startsWith('pod-in:')) return { pod: handle.slice('pod-in:'.length) };
	if (node.startsWith('exit:')) return { exit: node.slice('exit:'.length) };
	if (node.startsWith('server:')) {
		return pickTarget({
			title: m.editor_target_new_relay_title(),
			scope: canvasId,
			server: node.slice('server:'.length)
		});
	}
	const inside = node.startsWith('canvas:')
		? node.slice('canvas:'.length)
		: node.startsWith('portal:')
			? node.slice('portal:'.length)
			: null;
	if (inside === null) return null;
	return pickTarget({
		title: m.editor_target_title(),
		scope: inside,
		excludePods: exclude
	});
}

/**
 * A splitter's member row or an aggregator's way out: a drag from it gives
 * every pod behind it a way on at once (see `routeNodesAt`).
 */
const isRow = (from: Handle) =>
	(from.node.startsWith('split:') || from.node.startsWith('agg:')) &&
	from.handle.startsWith('out:');

/**
 * A pod's new way on. A relay pod that had none, landed together with relay
 * pods that have none either, offers to connect those the same way — in the
 * toast, so nothing waits on it.
 */
async function connectPod(current: CanvasGraph, podId: string, target: Target) {
	const pod = current.pods.find(entry => entry.id === podId);
	const bare = pod !== undefined && pod.route === null && isRelay(pod.ingress);
	const batch = connect(current, podId, target);
	if (!(await commit(batch, undefined, current))) return;
	const onward = targetOf(batch, podId);
	const siblings = bare && onward ? fanOutSiblings(current, podId, onward) : [];
	if (!onward || siblings.length === 0) {
		toast.success(m.editor_connected());
		return;
	}
	toast.success(m.editor_connected(), {
		description: m.editor_siblings_hint({ count: siblings.length }),
		duration: 10_000,
		action: {
			label: m.editor_connect_siblings({ count: siblings.length }),
			onClick: () => void connectSiblings(podId, onward)
		}
	});
}

/** The siblings of `podId` as the graph has them now, connected to `target` in one batch. */
async function connectSiblings(podId: string, target: Target) {
	const current = graph;
	if (!current) return;
	try {
		const siblings = fanOutSiblings(current, podId, target);
		if (siblings.length === 0) return;
		const batch = connectEach(current, siblings, target);
		if (await commit(batch, undefined, current)) {
			toast.success(m.editor_siblings_connected({ count: siblings.length }));
		}
	} catch (err) {
		report(err);
	}
}

/**
 * A connection drawn on the canvas, as a batch. Svelte Flow inserts no edge of
 * its own: the buses are redrawn from the graph the control plane kept.
 */
async function connectGesture(connection: Connection) {
	const current = graph;
	const currentDrawing = drawing;
	if (!current || !currentDrawing) return;
	const sourceHandle = connection.sourceHandle ?? '';
	const targetHandle = connection.targetHandle ?? '';
	const from = { node: connection.source, handle: sourceHandle };
	try {
		if (sourceHandle.startsWith('pod-out:')) {
			const podId = sourceHandle.slice('pod-out:'.length);
			if (connection.target.startsWith('split:')) {
				await commit(
					() => joinSplitter(current, currentDrawing, podId, connection.target),
					m.editor_joined_splitter()
				);
				return;
			}
			const target = await dropTarget(connection.target, targetHandle, [podId]);
			if (target) await connectPod(current, podId, target);
		} else if (sourceHandle === 'add' && connection.source.startsWith('split:')) {
			const target = await dropTarget(connection.target, targetHandle, []);
			if (target) {
				await commit(
					() => addSplitterMember(current, currentDrawing, connection.source, target),
					m.editor_member_added(),
					current
				);
			}
		} else if (isRow(from)) {
			const rowPods = [...new Set(routeNodesAt(currentDrawing, from).map(node => node.podId))];
			const target = await dropTarget(connection.target, targetHandle, rowPods);
			if (!target) return;
			const batch = addWayOn(current, currentDrawing, from, target);
			if (await commit(batch, undefined, current)) {
				toast.success(m.editor_ways_added({ count: batch.putEdges.length }));
			}
		}
	} catch (err) {
		report(err);
	}
}

const isValidConnection = (connection: Edge | Connection): boolean => {
	const sourceHandle = connection.sourceHandle ?? '';
	const targetHandle = connection.targetHandle ?? '';
	const from = { node: connection.source, handle: sourceHandle };
	if (isRow(from)) {
		return (
			graph !== undefined &&
			drawing !== undefined &&
			canDropWayOn(graph, drawing, from, { node: connection.target, handle: targetHandle })
		);
	}
	const fromPod = sourceHandle.startsWith('pod-out:');
	const fromSplitter = sourceHandle === 'add' && connection.source.startsWith('split:');
	if (!fromPod && !fromSplitter) return false;
	if (targetHandle.startsWith('pod-in:')) {
		return (
			!fromPod || targetHandle.slice('pod-in:'.length) !== sourceHandle.slice('pod-out:'.length)
		);
	}
	if (targetHandle !== 'in') return false;
	if (connection.target.startsWith('split:')) {
		if (!fromPod) return false;
		const card = drawing?.cards.find(entry => entry.id === connection.target);
		const podId = sourceHandle.slice('pod-out:'.length);
		return card?.kind === 'splitter' && !card.members.some(member => member.podId === podId);
	}
	return ['server:', 'exit:', 'canvas:', 'portal:'].some(prefix =>
		connection.target.startsWith(prefix)
	);
};

// --- removing ----------------------------------------------------------------

/**
 * What a selection takes away, shown before it goes. Servers and subcanvases are
 * deleted after the batch that removes what runs on them and leads into them.
 */
function requestRemoval(nodeIds: string[], busIds: string[]) {
	const current = graph;
	const currentDrawing = drawing;
	if (!current || !currentDrawing || !editable) return;
	const serverIds = nodeIds
		.filter(id => id.startsWith('server:'))
		.map(id => id.slice('server:'.length))
		.filter(id => current.servers.some(server => server.id === id && server.canvasId === canvasId));
	const canvasIds = nodeIds
		.filter(id => id.startsWith('canvas:'))
		.map(id => id.slice('canvas:'.length));
	const exitIds = nodeIds.filter(id => id.startsWith('exit:')).map(id => id.slice('exit:'.length));
	const splitterIds = nodeIds.filter(id => id.startsWith('split:'));
	const edgeIds = busIds.flatMap(
		id => currentDrawing.buses.find(bus => bus.id === id)?.edges ?? []
	);
	if (
		serverIds.length + canvasIds.length + exitIds.length + splitterIds.length + edgeIds.length ===
		0
	) {
		return;
	}
	const names = (ids: string[], rows: { id: string; name: string }[]) =>
		ids.map(id => rows.find(row => row.id === id)?.name ?? id);
	reviewRequest = {
		title: m.editor_remove_title(),
		description: m.editor_remove_description(),
		prunable: true,
		// Built from the live graph, not the one the request was made from: the
		// dialog commits against the generation of the graph it reads, so a batch
		// built from an older one would write stale rows over a concurrent edit.
		build: prune => {
			const now = graph ?? current;
			const nowDrawing = drawing ?? currentDrawing;
			return removeAll(
				now,
				nowDrawing,
				{
					podIds: now.pods.filter(pod => serverIds.includes(pod.serverId)).map(pod => pod.id),
					exitIds,
					splitterIds,
					edgeIds: busIds.flatMap(id => nowDrawing.buses.find(bus => bus.id === id)?.edges ?? [])
				},
				prune
			);
		},
		alsoDeletes: {
			servers: names(serverIds, current.servers),
			canvases: names(canvasIds, current.canvases)
		},
		after: async () => {
			for (const serverId of serverIds) await deleteServerNode({ serverId });
			for (const subcanvasId of canvasIds) await deleteSubcanvas({ canvasId, subcanvasId });
		},
		success: m.editor_deleted()
	};
}

/** Always `false`: deletions are batches the operator confirms first. */
async function beforeDelete({ nodes: doomed, edges: cut }: { nodes: FlowNode[]; edges: Edge[] }) {
	requestRemoval(
		doomed.map(node => node.id),
		cut.map(edge => edge.id)
	);
	return false;
}

// --- moving ------------------------------------------------------------------

async function persistMoves(dragged: FlowNode[]) {
	const current = graph;
	if (!current || dragged.length === 0) return;
	const moves = moveCards(
		current,
		canvasId,
		dragged.map(node => ({ node: node.id, position: node.position }))
	);
	try {
		if (moves.servers.length + moves.exits.length + moves.canvases.length > 0) {
			await moveCanvasItems({
				canvasId,
				servers: moves.servers,
				exits: moves.exits,
				canvases: moves.canvases
			});
		}
		// Rows moved without a batch need no re-read: the move announces itself,
		// and its snapshot arrives on the open stream.
		if (moves.layout) {
			const outcome = await applyGraphChange({ canvasId, change: moves.layout });
			if (!outcome.applied) toast.error(refusalText(outcome.diagnostics));
		}
	} catch (err) {
		report(err);
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
 * flow count: the panel's inputs handle their own arrows.
 */
function nudgeStop(event: KeyboardEvent) {
	if (!editable || !view.interactive || !ARROW_KEYS[event.key]) return;
	if (!(event.target instanceof Node) || !flowEl?.contains(event.target)) return;
	persistMoves(nodes.filter(node => node.selected));
}

// --- opening -----------------------------------------------------------------

/**
 * Svelte Flow has no double-click event and delivers a pointer event whose
 * `detail` is always 0, so the second click is recognised here: same card,
 * within the usual double-click window.
 */
const DOUBLE_CLICK_MS = 400;
let lastClick: { id: string; at: number } | null = null;

function clickCard(node: FlowNode, event?: MouseEvent | TouchEvent) {
	const row = event?.target instanceof Element ? event.target.closest('[data-pod-id]') : null;
	const podId = row?.getAttribute('data-pod-id');
	if (podId) {
		panelTarget = { kind: 'pod', id: podId };
		return;
	}
	const now = Date.now();
	const again = lastClick?.id === node.id && now - lastClick.at < DOUBLE_CLICK_MS;
	lastClick = again ? null : { id: node.id, at: now };
	const [kind, ...rest] = node.id.split(':');
	const id = rest.join(':');
	if (again && (kind === 'canvas' || kind === 'portal')) {
		goto(`/canvas/${id}`);
		return;
	}
	switch (kind) {
		case 'server':
		case 'exit':
		case 'canvas':
			panelTarget = { kind, id };
			break;
		case 'portal':
			panelTarget = { kind: 'portal', id: node.id };
			break;
		case 'split': {
			const card = drawing?.cards.find(entry => entry.id === node.id);
			const anchor = card?.kind === 'splitter' ? card.members[0] : undefined;
			panelTarget = { kind: 'splitter', id: node.id, anchor };
			break;
		}
		case 'agg': {
			const card = drawing?.cards.find(entry => entry.id === node.id);
			const anchor = card?.kind === 'aggregator' ? card.targets[0] : undefined;
			panelTarget = { kind: 'aggregator', id: node.id, anchor };
			break;
		}
	}
}

function clickBus(id: string) {
	const anchor = drawing?.buses.find(bus => bus.id === id)?.edges[0];
	panelTarget = { kind: 'bus', id, anchor };
}

/**
 * Opens what a diagnostic is about: its card and panel here, or the canvas of
 * the tree it lives on, which picks the handover up from `?focus`.
 */
function openSubject(subject: Subject) {
	const current = graph;
	const currentDrawing = drawing;
	if (!current || !currentDrawing) return;
	const where = locate(current, currentDrawing, subject);
	if (where.kind === 'canvas') {
		if (where.canvasId !== canvasId) {
			const [key, value] = Object.entries(subject)[0] ?? [];
			goto(`/canvas/${where.canvasId}?focus=${key}:${encodeURIComponent(String(value))}`);
		}
		return;
	}
	if (where.kind === 'bus') {
		clickBus(where.bus);
		return;
	}
	if (where.kind !== 'card') return;
	updateNode(where.node, { selected: true });
	if ('pod' in subject && where.node.startsWith('server:')) {
		panelTarget = { kind: 'pod', id: subject.pod };
		return;
	}
	const node = nodes.find(entry => entry.id === where.node);
	if (node) clickCard(node);
}

/**
 * The handover from a diagnostic opened on another canvas of the tree. It runs
 * once the graph is there, then drops the parameter so a reload does not
 * re-select something the operator has since moved on from.
 */
let focusHandled = '';
$effect(() => {
	const wanted = page.url.searchParams.get('focus');
	if (!wanted) {
		focusHandled = '';
		return;
	}
	if (!graph || focusHandled === wanted) return;
	focusHandled = wanted;
	const separator = wanted.indexOf(':');
	const key = wanted.slice(0, separator);
	const value = wanted.slice(separator + 1);
	if (['server', 'pod', 'exit', 'edge', 'group', 'canvas'].includes(key)) {
		untrack(() => openSubject({ [key]: value } as Subject));
	}
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
	{#if graph === undefined && query.error}
		<div class="grid h-full place-items-center p-4">
			<BoundaryError error={query.error} retry />
		</div>
	{:else if graph === undefined || drawing === undefined}
		<Skeleton class="h-full w-full" />
	{:else}
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
						onbeforeconnect={connection => {
							connectGesture(connection);
							return null;
						}}
						onbeforedelete={beforeDelete}
						onnodeclick={({ node, event }) => clickCard(node, event)}
						onedgeclick={({ edge }) => clickBus(edge.id)}
						onnodedragstop={({ nodes: dragged }) => persistMoves(dragged)}
						onmove={(_, viewport) => (view.zoom = viewport.zoom)}
						deleteKey={editable && view.interactive ? 'Delete' : null}
					>
						<Background />
						<MiniMap />

						<Panel position="top-left" class="flex items-center gap-2">
							<CanvasMenubar
								{editable}
								{view}
								onAddServer={addServer}
								onAddExit={() => (exitOpen = true)}
								onAddSubcanvas={() => (subcanvasOpen = true)}
							/>
							<LiveBadge {query} />
						</Panel>

						<Panel position="bottom-left">
							<CanvasProblems {graph} {drawing} onopen={openSubject} />
						</Panel>

						<Panel position="top-right">
							<RuleLegend {graph} {drawing} bind:highlighted={highlightedRule} />
						</Panel>

						<!-- A re-read that failed leaves the last graph on screen: say so. -->
						{#if query.error}
							<Panel position="top-center" class="w-96 max-w-[calc(100%-2rem)]">
								<BoundaryError
									error={query.error}
									retry
									variant="inline"
									title={m.error_stale_title()}
								/>
							</Panel>
						{/if}
					</SvelteFlow>

					{#if drawing.cards.length === 0}
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
					<InspectorPanel bind:target={panelTarget} />
				</Resizable.Pane>
			{/if}
		</Resizable.PaneGroup>

		<ReviewDialog bind:request={reviewRequest} {graph} {canvasId} />
		<TargetDialog
			request={pick?.request ?? null}
			{graph}
			onpick={target => {
				pick?.resolve(target);
				pick = null;
			}}
		/>
		{#if editable}
			<AddSubcanvasDialog
				bind:open={subcanvasOpen}
				{canvasId}
				place={spotFor('canvas')}
				suggest={() => suggestName(m.editor_kind_subcanvas(), getLocale(), usedNames(graph))}
			/>
			<AddExitDialog
				bind:open={exitOpen}
				place={spotFor('exit')}
				suggest={() => suggestName(m.editor_add_exit(), getLocale(), usedNames(graph))}
			/>
		{/if}
	{/if}

	{#snippet failed(error, reset)}
		<BoundaryError {error} {reset} />
	{/snippet}
</svelte:boundary>
