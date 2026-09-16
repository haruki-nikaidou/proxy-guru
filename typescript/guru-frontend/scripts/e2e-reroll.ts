// Moves every landing pod on a server whose port falls in [lo, hi] to a port
// outside that range. Usage: bun scripts/e2e-reroll.ts <email> <password> <canvasId> <server-name> <lo> <hi>
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { OrchestrationDefinition } from 'app-protobuf/orchestration/orchestration';
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';

const [email, password, canvasId, serverName, lo, hi] = process.argv.slice(2);
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({
	email: email ?? '',
	password: password ?? '',
	userAgent: 'e2e-reroll'
});
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const detail = await orch.getCanvas({ canvasId: canvasId ?? '' }, opts);
const server = detail.servers.find(s => s.name === serverName);
if (!server) throw new Error(`server ${serverName} not found`);
const used = new Set<number>();
for (const n of detail.nodes) {
	const pod = n.spec?.pod;
	if (pod && pod.serverId === server.id) used.add(pod.port);
}
const [low, high] = [Number(lo), Number(hi)];
const free = () => {
	let p: number;
	do {
		p = 40000 + Math.floor(Math.random() * 20000);
	} while ((p >= low && p <= high) || used.has(p));
	used.add(p);
	return p;
};
for (const node of detail.nodes) {
	const pod = node.spec?.pod;
	if (!pod || pod.serverId !== server.id || pod.port < low || pod.port > high) continue;
	const port = free();
	await orch.replaceNodeSpec(
		{ nodeId: node.id, spec: { pod: { ...pod, port } }, itemCount: 0 },
		opts
	);
	console.log(`${node.name}: ${pod.port} -> ${port}`);
}
process.exit(0);
