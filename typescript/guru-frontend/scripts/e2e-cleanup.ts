// Removes the servers the UI drag test created: disconnect their bundles, then delete.
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { OrchestrationDefinition } from 'app-protobuf/orchestration/orchestration';
const [email, password, canvasId, prefix] = process.argv.slice(2);
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({ email: email!, password: password!, userAgent: 'e2e-cleanup' });
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const detail = await orch.getCanvas({ canvasId: canvasId! }, opts);
for (const server of detail.servers) {
	if (!server.name.startsWith(prefix!)) continue;
	const up = detail.nodes.find(n => n.spec?.universalPod?.serverId === server.id);
	const portIds = new Set((up?.ports ?? []).map(p => p.id));
	for (const edge of detail.edges) {
		if (portIds.has(edge.sourcePortId) || portIds.has(edge.targetPortId)) {
			await orch.disconnect({ edgeId: edge.id }, opts);
			console.log('disconnected', edge.id);
		}
	}
	await orch.deleteServer({ serverId: server.id }, opts);
	console.log('deleted', server.name);
}
const problems = (await orch.validateCanvas({ canvasId: canvasId! }, opts)).problems;
console.log(problems.map(p => p.message));
process.exit(0);
