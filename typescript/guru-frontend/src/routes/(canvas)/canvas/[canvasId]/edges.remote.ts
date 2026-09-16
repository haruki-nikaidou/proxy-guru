/**
 * The edges between ports. Drawing one is the only write that may create ports
 * of its own: the control plane makes the port behind a universal node's handle
 * group, and the lanes that follow from it, in the same write as the edge.
 */
import * as v from 'valibot';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { fromGroup } from '#lib/server/topology/enums.js';
import { connectEndSchema, idSchema } from '#lib/server/topology/schemas.js';
import { command } from '$app/server';
import { getCanvasGraph } from './topology.remote.js';

/**
 * Connects two ports, or a universal node's handle group to a port: the port
 * behind a group is created by the control plane in the same write as the
 * edge and the lanes it calls for. A bundle leaves through a port that exists
 * (a member, a universal pod's `bundle out`) and lands on a `+ bundle` handle
 * or on an aggregate node's member.
 */
export const connectNodePorts = command(
	v.object({ canvasId: idSchema, output: connectEndSchema, input: connectEndSchema }),
	async ({ canvasId, output, input }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().connectPorts(
				{
					outputPortId: 'portId' in output ? output.portId : '',
					inputPortId: 'portId' in input ? input.portId : '',
					outputHandle:
						'portId' in output
							? undefined
							: { nodeId: output.nodeId, group: fromGroup(output.group) },
					inputHandle:
						'portId' in input ? undefined : { nodeId: input.nodeId, group: fromGroup(input.group) }
				},
				{ metadata }
			)
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);

export const disconnectEdge = command(
	v.object({ canvasId: idSchema, edgeId: idSchema, force: v.optional(v.boolean(), false) }),
	async ({ canvasId, edgeId, force }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			force
				? orchestrationClient().forceDisconnect({ edgeId }, { metadata })
				: orchestrationClient().disconnect({ edgeId }, { metadata })
		);
		await getCanvasGraph({ canvasId }).refresh();
		return { ok: true as const };
	}
);
