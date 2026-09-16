import type {
	GetGraphReply,
	CanvasTreeNode as ProtoCanvasTreeNode
} from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type {
	CanvasOption,
	CanvasOptionEntry,
	CanvasProblem,
	CanvasSummary,
	CanvasTrail,
	CanvasTreeNode
} from '#lib/dto/canvas.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, form, query } from '$app/server';

// The control plane only rejects empty names; every other limit below is this
// dashboard's own.
const nameSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'canvas_name_required'),
	v.maxLength(128, 'canvas_name_too_long')
);
const descriptionSchema = v.optional(
	v.pipe(v.string(), v.trim(), v.maxLength(1000, 'canvas_description_too_long')),
	''
);

const PROBLEM_LIMIT = 5;

/**
 * One canvas's share of its tree: the servers, pods and exits placed on it, the
 * edges leaving its pods, and the diagnostics about any of them. A diagnostic
 * naming nothing belongs to the root.
 */
function summarise(graph: GetGraphReply, canvasId: string, isRoot: boolean) {
	const pods = new Map(graph.pods.map(pod => [pod.id, pod.canvasId]));
	const servers = new Map(graph.servers.map(server => [server.id, server.canvasId]));
	const exits = new Map(graph.exits.map(exit => [exit.id, exit.canvasId]));
	const edges = new Map(graph.edges.map(edge => [edge.id, pods.get(edge.sourcePodId)]));
	const groups = new Map(graph.groups.map(group => [group.id, group.canvasId]));
	const here = graph.diagnostics.filter(diagnostic =>
		diagnostic.subjects.length === 0
			? isRoot
			: diagnostic.subjects.some(
					subject =>
						(subject.podId !== undefined && pods.get(subject.podId) === canvasId) ||
						(subject.serverId !== undefined && servers.get(subject.serverId) === canvasId) ||
						(subject.exitId !== undefined && exits.get(subject.exitId) === canvasId) ||
						(subject.edgeId !== undefined && edges.get(subject.edgeId) === canvasId) ||
						(subject.groupId !== undefined && groups.get(subject.groupId) === canvasId) ||
						subject.canvasId === canvasId
				)
	);
	const problems: CanvasProblem[] = here.map(diagnostic => ({
		severity: diagnostic.error ? 'error' : 'warning',
		kind: diagnostic.problem,
		message: diagnostic.message
	}));
	return {
		stats: {
			servers: graph.servers.filter(server => server.canvasId === canvasId).length,
			pods: graph.pods.filter(pod => pod.canvasId === canvasId).length,
			edges: [...edges.values()].filter(owner => owner === canvasId).length
		},
		health: {
			errors: problems.filter(problem => problem.severity === 'error').length,
			warnings: problems.filter(problem => problem.severity === 'warning').length,
			// Errors first: they are what blocks the next edit.
			problems: [...problems]
				.sort((a, b) => (a.severity === b.severity ? 0 : a.severity === 'error' ? -1 : 1))
				.slice(0, PROBLEM_LIMIT)
		}
	};
}

function toTreeNode(node: ProtoCanvasTreeNode | undefined): CanvasTreeNode {
	const canvas = node?.canvas;
	return {
		canvas: {
			id: canvas?.id ?? '',
			name: canvas?.name ?? '',
			description: canvas?.description ?? ''
		},
		children: (node?.children ?? []).map(toTreeNode)
	};
}

/** The canvases from the tree root down to `canvasId`, that canvas last. */
function pathTo(node: CanvasTreeNode, canvasId: string): CanvasOption[] | null {
	if (node.canvas.id === canvasId) return [node.canvas];
	for (const child of node.children) {
		const below = pathTo(child, canvasId);
		if (below) return [node.canvas, ...below];
	}
	return null;
}

/**
 * Names only: what the editor's canvas switcher and settings page need. Kept
 * apart from `listCanvases` so opening a canvas does not pay for the per-tree
 * graph reads behind the dashboard cards.
 *
 * Subcanvases are listed too — a subcanvas has its own settings page and its
 * own title — and `isRoot` is the difference between the two cheap listings, so
 * the switcher can still offer roots only.
 */
export const listCanvasOptions = query(async (): Promise<CanvasOptionEntry[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const { canvases } = await callGrpc(() =>
		orchestrationClient().listCanvases({ includeSubcanvases: true }, { metadata })
	);
	return canvases
		.map(canvas => ({
			id: canvas.id,
			name: canvas.name,
			description: canvas.description,
			isRoot: canvas.parentId === ''
		}))
		.sort((a, b) => a.name.localeCompare(b.name));
});

export const listCanvases = query(
	v.object({ includeSubcanvases: v.optional(v.boolean(), false) }),
	async ({ includeSubcanvases }): Promise<CanvasSummary[]> => {
		const metadata = sessionMetadata(requireSessionId());

		// Every canvas, whichever listing is asked for: a subcanvas's card names its
		// parent, and its stats come from its tree's graph.
		const { canvases } = await callGrpc(() =>
			orchestrationClient().listCanvases({ includeSubcanvases: true }, { metadata })
		);
		const byId = new Map(canvases.map(canvas => [canvas.id, canvas]));
		const rootOf = (id: string): string => {
			let current = byId.get(id);
			for (let depth = 0; current?.parentId && depth < 64; depth += 1) {
				const parent = byId.get(current.parentId);
				if (!parent) break;
				current = parent;
			}
			return current?.id ?? id;
		};
		const listed = canvases
			.filter(canvas => includeSubcanvases || canvas.parentId === '')
			// The backend returns no ordering and no timestamps to sort by.
			.sort((a, b) => a.name.localeCompare(b.name));

		// One graph per tree. Deliberately outside `callGrpc`: a tree deleted
		// between the list and this fan-out must degrade its cards, not 404 the page.
		const graphs = new Map<string, Promise<GetGraphReply | null>>();
		for (const canvas of listed) {
			const root = rootOf(canvas.id);
			if (!graphs.has(root)) {
				graphs.set(
					root,
					orchestrationClient()
						.getGraph({ canvasId: root }, { metadata })
						.catch(() => null)
				);
			}
		}

		return Promise.all(
			listed.map(async (canvas): Promise<CanvasSummary> => {
				const base = { id: canvas.id, name: canvas.name, description: canvas.description };
				const parent = canvas.parentId === '' ? undefined : byId.get(canvas.parentId);
				const graph = await graphs.get(rootOf(canvas.id));
				return {
					...base,
					...(graph
						? summarise(graph, canvas.id, canvas.parentId === '')
						: { stats: null, health: null }),
					parent: parent
						? { id: parent.id, name: parent.name, description: parent.description }
						: null
				};
			})
		);
	}
);

/**
 * The nesting tree a canvas belongs to, plus the path from its root down to it.
 * One `GetCanvasTree` call: the reply is always the whole tree, whichever
 * canvas of it was asked about.
 */
export const getCanvasTrail = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasTrail> => {
		const metadata = sessionMetadata(requireSessionId());
		const { root } = await callGrpc(() =>
			orchestrationClient().getCanvasTree({ canvasId }, { metadata })
		);
		const tree = toTreeNode(root);
		return { root: tree, path: pathTo(tree, canvasId) ?? [tree.canvas] };
	}
);

/**
 * Refreshes both listings: the dashboard toggles between them. Private, like
 * every non-remote value in a `.remote.ts` module.
 */
const refreshCanvasLists = () =>
	Promise.all([
		listCanvases({ includeSubcanvases: false }).refresh(),
		listCanvases({ includeSubcanvases: true }).refresh(),
		listCanvasOptions().refresh()
	]);

export const createCanvas = form(
	v.object({ name: nameSchema, description: descriptionSchema }),
	async ({ name, description }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().createCanvas({ name, description }, { metadata }));
		await refreshCanvasLists();
		return { ok: true as const };
	}
);

export const updateCanvas = form(
	v.object({ canvasId: idSchema, name: nameSchema, description: descriptionSchema }),
	async ({ canvasId, name, description }) => {
		const metadata = sessionMetadata(requireSessionId());
		// UpdateCanvas is a full replace: omitting the description clears it.
		await callGrpc(() =>
			orchestrationClient().updateCanvas({ canvasId, name, description }, { metadata })
		);
		await refreshCanvasLists();
		return { ok: true as const };
	}
);

export const deleteCanvas = command(v.object({ canvasId: idSchema }), async ({ canvasId }) => {
	const metadata = sessionMetadata(requireSessionId());
	await callGrpc(() => orchestrationClient().deleteCanvas({ canvasId }, { metadata }));
	await refreshCanvasLists();
	return { ok: true as const };
});
