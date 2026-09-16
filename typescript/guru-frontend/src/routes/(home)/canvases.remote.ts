import type { CanvasTreeNode as ProtoCanvasTreeNode } from 'app-protobuf/orchestration/orchestration';
import { ProblemKind, ProblemSeverity } from 'app-protobuf/orchestration/orchestration';
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

function toProblem(severity: ProblemSeverity, kind: ProblemKind, message: string): CanvasProblem {
	return {
		severity:
			severity === ProblemSeverity.PROBLEM_ERROR
				? 'error'
				: severity === ProblemSeverity.PROBLEM_WARNING
					? 'warning'
					: 'unknown',
		kind: ProblemKind[kind] ?? 'UNSPECIFIED',
		message
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
 * apart from `listCanvases` so opening a canvas does not pay for the per-canvas
 * detail/validate fan-out behind the dashboard cards.
 *
 * Subcanvases are listed too — a subcanvas has its own settings page and its
 * own title — and `isRoot` is the difference between the two cheap listings, so
 * the switcher can still offer roots only.
 */
export const listCanvasOptions = query(async (): Promise<CanvasOptionEntry[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const [all, roots] = await callGrpc(() =>
		Promise.all([
			orchestrationClient().listCanvases({ includeSubcanvases: true }, { metadata }),
			orchestrationClient().listCanvases({ includeSubcanvases: false }, { metadata })
		])
	);
	const rootIds = new Set(roots.canvases.map(canvas => canvas.id));
	return all.canvases
		.map(canvas => ({
			id: canvas.id,
			name: canvas.name,
			description: canvas.description,
			isRoot: rootIds.has(canvas.id)
		}))
		.sort((a, b) => a.name.localeCompare(b.name));
});

export const listCanvases = query(
	v.object({ includeSubcanvases: v.optional(v.boolean(), false) }),
	async ({ includeSubcanvases }): Promise<CanvasSummary[]> => {
		const metadata = sessionMetadata(requireSessionId());

		const { canvases } = await callGrpc(() =>
			orchestrationClient().listCanvases({ includeSubcanvases }, { metadata })
		);
		// The backend returns no ordering and no timestamps to sort by.
		const ordered = [...canvases].sort((a, b) => a.name.localeCompare(b.name));

		return Promise.all(
			ordered.map(async (canvas): Promise<CanvasSummary> => {
				const base = { id: canvas.id, name: canvas.name, description: canvas.description };
				try {
					// Deliberately outside `callGrpc`: a canvas deleted between the list and
					// this fan-out must degrade one card, not 404 the whole page.
					const [detail, validation] = await Promise.all([
						orchestrationClient().getCanvas({ canvasId: canvas.id }, { metadata }),
						orchestrationClient().validateCanvas({ canvasId: canvas.id }, { metadata })
					]);

					let errors = 0;
					let warnings = 0;
					for (const problem of validation.problems) {
						if (problem.severity === ProblemSeverity.PROBLEM_ERROR) errors += 1;
						else if (problem.severity === ProblemSeverity.PROBLEM_WARNING) warnings += 1;
					}

					// `ancestors` is root first, parent last.
					const parent = detail.ancestors.at(-1);
					return {
						...base,
						stats: {
							servers: detail.servers.length,
							nodes: detail.nodes.length,
							edges: detail.edges.length
						},
						health: {
							errors,
							warnings,
							problems: validation.problems
								.slice(0, PROBLEM_LIMIT)
								.map(problem => toProblem(problem.severity, problem.kind, problem.message))
						},
						parent: parent
							? { id: parent.id, name: parent.name, description: parent.description }
							: null
					};
				} catch {
					return { ...base, stats: null, health: null, parent: null };
				}
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
