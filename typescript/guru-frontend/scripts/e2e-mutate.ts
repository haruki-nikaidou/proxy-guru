// Mutations for the universal e2e: `protocol <tcp_raw|tcp_tls|quic>` on the
// distribute node, or `unbundle <server-name>` (cuts the bundle into that server).
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { LoadBalanceMode, OrchestrationDefinition, RelayProtocol } from 'app-protobuf/orchestration/orchestration';
const [email, password, canvasId, action, arg] = process.argv.slice(2);
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({ email: email!, password: password!, userAgent: 'e2e-mutate' });
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const detail = await orch.getCanvas({ canvasId: canvasId! }, opts);
if (action === 'protocol') {
	const ud = detail.nodes.find(n => n.spec?.loadBalanceDistribute && n.ports.some(p => p.key.startsWith('chan:')))!;
	const protocol = arg === 'quic' ? RelayProtocol.RELAY_QUIC : arg === 'tcp_tls' ? RelayProtocol.RELAY_TCP_TLS : RelayProtocol.RELAY_TCP_RAW;
	await orch.replaceNodeSpec({ nodeId: ud.id, spec: { loadBalanceDistribute: { mode: LoadBalanceMode.ROUND_ROBIN, protocol } }, itemCount: 0 }, opts);
} else if (action === 'unbundle') {
	const server = detail.servers.find(s => s.name === arg)!;
	const up = detail.nodes.find(n => n.spec?.universalPod?.serverId === server.id)!;
	const ins = new Set(up.ports.filter(p => p.key.startsWith('bundle_in:')).map(p => p.id));
	for (const edge of detail.edges) if (ins.has(edge.targetPortId)) await orch.disconnect({ edgeId: edge.id }, opts);
}
const after = await orch.getCanvas({ canvasId: canvasId! }, opts);
const lanes = after.nodes.filter(n => n.lane).map(n => `${n.lane!.role}:${n.name}${n.spec?.pod ? ':' + n.spec.pod.port : ''}${n.spec?.relay ? ':' + RelayProtocol[n.spec.relay.protocol] : ''}`).sort();
console.log(JSON.stringify({ lanes, problems: (await orch.validateCanvas({ canvasId: canvasId! }, opts)).problems.map(p => p.message) }, null, 1));
process.exit(0);
