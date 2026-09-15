// Asks every server of every canvas to update to the published agent release and
// waits until each registers as that version or reports an error.
// Usage: bun scripts/agent-rollout.ts <email> <password> [timeoutSecs]

import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { OrchestrationDefinition } from 'app-protobuf/orchestration/orchestration';
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';

const [email, password, timeoutArg] = process.argv.slice(2);
const timeoutSecs = Number(timeoutArg ?? '300');
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({
	email: email ?? '',
	password: password ?? '',
	userAgent: 'agent-rollout'
});
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };

const release = await orch.getAgentRelease({}, opts);
if (!release.version) throw new Error('no published agent release');
const target = release.version;
console.log('published release', target);

const canvases = (await orch.listCanvases({}, opts)).canvases;
const servers: { id: string; name: string }[] = [];
for (const canvas of canvases) {
	const detail = await orch.getCanvas({ canvasId: canvas.id }, opts);
	for (const server of detail.servers) {
		servers.push({ id: server.id, name: server.name });
		if (server.agentVersion === target) {
			console.log(server.name, 'already at', target);
			continue;
		}
		await orch.requestAgentUpdate({ serverId: server.id }, opts);
		console.log(server.name, server.agentVersion, '→', target, 'requested');
	}
}

const deadline = Date.now() + timeoutSecs * 1000;
const pending = new Set(servers.map(s => s.id));
while (pending.size > 0 && Date.now() < deadline) {
	await new Promise(r => setTimeout(r, 5000));
	for (const canvas of canvases) {
		const detail = await orch.getCanvas({ canvasId: canvas.id }, opts);
		for (const server of detail.servers) {
			if (!pending.has(server.id)) continue;
			if (server.agentUpdateError) {
				console.log(server.name, 'FAILED:', server.agentUpdateError);
				pending.delete(server.id);
			} else if (server.agentVersion === target && !server.agentUpdateRequested) {
				console.log(server.name, 'now', target, 'health', server.healthStatus);
				pending.delete(server.id);
			}
		}
	}
}
for (const id of pending)
	console.log(servers.find(s => s.id === id)?.name, 'still pending after', timeoutSecs, 's');
process.exit(pending.size === 0 ? 0 : 1);
