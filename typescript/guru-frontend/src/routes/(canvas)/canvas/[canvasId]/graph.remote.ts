/**
 * The pod graph of a canvas tree: reading it, changing it, moving what is drawn
 * of it. Every edit on the canvas — a connection, a policy, a delete — is one
 * `applyGraphChange` batch the control plane checks as a whole; the gestures
 * that compute those batches live in `guru-graph`.
 *
 * The control plane is authoritative on permissions: no role check happens here.
 */
import * as v from 'valibot';
import type { ApplyOutcomeDto, CanvasGraph } from '#lib/dto/topology.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { toApplyOutcome, toCanvasGraph } from '#lib/server/topology/decode.js';
import { fromChange } from '#lib/server/topology/encode.js';
import { graphChangeSchema, positionsSchema } from '#lib/server/topology/schemas.js';
import { command, query } from '$app/server';

/** The whole tree `canvasId` belongs to, with the diagnostics its graph checks with. */
export const getCanvasGraph = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasGraph> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() => orchestrationClient().getGraph({ canvasId }, { metadata }));
		return toCanvasGraph(reply);
	}
);

/**
 * One batch of changes, all of it or none. `dryRun` answers what the batch
 * would lead to without writing; `expectedGeneration` refuses a batch computed
 * against a tree that has moved on since (0 skips the check). A refused batch is
 * not an error: the outcome says `applied: false` and carries the diagnostics.
 */
export const applyGraphChange = command(
	v.object({
		canvasId: idSchema,
		change: graphChangeSchema,
		dryRun: v.optional(v.boolean(), false),
		expectedGeneration: v.optional(v.pipe(v.number(), v.integer(), v.minValue(0)), 0)
	}),
	async ({ canvasId, change, dryRun, expectedGeneration }): Promise<ApplyOutcomeDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().applyGraph(
				{
					canvasId,
					change: fromChange(change),
					dryRun,
					expectedGeneration: BigInt(expectedGeneration)
				},
				{ metadata }
			)
		);
		const outcome = toApplyOutcome(reply);
		if (outcome.applied) await getCanvasGraph({ canvasId }).refresh();
		return outcome;
	}
);

/**
 * Where servers, exits and subcanvases are drawn. Deliberately does not refresh:
 * the dragged positions already match on screen.
 */
export const moveCanvasItems = command(
	v.object({
		canvasId: idSchema,
		servers: positionsSchema,
		exits: positionsSchema,
		canvases: positionsSchema
	}),
	async ({ canvasId, servers, exits, canvases }) => {
		const metadata = sessionMetadata(requireSessionId());
		const positions = (items: { id: string; position: { x: number; y: number } }[]) =>
			items.map(item => ({
				id: item.id,
				position: { x: BigInt(item.position.x), y: BigInt(item.position.y) }
			}));
		await callGrpc(() =>
			orchestrationClient().moveItems(
				{
					canvasId,
					servers: positions(servers),
					exits: positions(exits),
					canvases: positions(canvases)
				},
				{ metadata }
			)
		);
		return { ok: true as const };
	}
);
