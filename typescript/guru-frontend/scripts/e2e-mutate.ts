// Mutations for the universal e2e: `protocol <tcp_raw|tcp_tls|quic>` on the
// distribute node that starts channels, `unbundle <server-name>` (cuts every
// bundle into that server), `members <node-name> <name,name,...>` (renames /
// appends / drops the members of a load-balance node by position: an existing
// slot keeps its number, a new name takes the next one), `exit <exit-name>
// <pod-name>` (connects that exit to the aggregate node's channel for the
// pod), or `noop` (lists).
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { OrchestrationDefinition, RelayProtocol } from 'app-protobuf/orchestration/orchestration';
const [email, password, canvasId, action, arg, arg2] = process.argv.slice(2);
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({ email: email!, password: password!, userAgent: 'e2e-mutate' });
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const detail = await orch.getCanvas({ canvasId: canvasId! }, opts);
if (action === 'protocol') {
	const ud = detail.nodes.find(n => n.spec?.loadBalanceDistribute && n.ports.some(p => p.key.startsWith('chan:')))!;
	const cfg = ud.spec!.loadBalanceDistribute!;
	const protocol = arg === 'quic' ? RelayProtocol.RELAY_QUIC : arg === 'tcp_tls' ? RelayProtocol.RELAY_TCP_TLS : RelayProtocol.RELAY_TCP_RAW;
	await orch.replaceNodeSpec({ nodeId: ud.id, spec: { loadBalanceDistribute: { ...cfg, protocol } }, itemCount: 0 }, opts);
} else if (action === 'unbundle') {
	const server = detail.servers.find(s => s.name === arg)!;
	const up = detail.nodes.find(n => n.spec?.universalPod?.serverId === server.id)!;
	const ins = new Set(up.ports.filter(p => p.key.startsWith('bundle_in:')).map(p => p.id));
	for (const edge of detail.edges) if (ins.has(edge.targetPortId)) await orch.disconnect({ edgeId: edge.id }, opts);
} else if (action === 'members') {
	const node = detail.nodes.find(n => n.name === arg)!;
	const current = node.spec?.loadBalanceDistribute?.members ?? node.spec?.loadBalanceAggregate?.members ?? [];
	let next = current.reduce((max, m) => Math.max(max, m.slot), 0);
	const members = (arg2 ?? '').split(',').map((name, i) => ({ slot: current[i]?.slot ?? ++next, name }));
	const spec = node.spec?.loadBalanceDistribute
		? { loadBalanceDistribute: { ...node.spec.loadBalanceDistribute, members } }
		: { loadBalanceAggregate: { members } };
	await orch.replaceNodeSpec({ nodeId: node.id, spec, itemCount: 0 }, opts);
} else if (action === 'exit') {
	const exit = detail.nodes.find(n => n.name === arg && n.spec?.exit)!;
	const pod = detail.nodes.find(n => n.name === arg2 && n.spec?.pod)!;
	const ua = detail.nodes.find(n => n.spec?.loadBalanceAggregate && !n.lane && n.ports.some(p => p.key === `chan:${pod.id}`))!;
	const chan = ua.ports.find(p => p.key === `chan:${pod.id}`)!;
	await orch.connectPorts({ outputPortId: exit.ports.find(p => p.key === 'destination')!.id, inputPortId: chan.id }, opts);
}
const after = await orch.getCanvas({ canvasId: canvasId! }, opts);
const lanes = after.nodes.filter(n => n.lane).map(n => `${n.lane!.role}:${n.name}${n.spec?.pod ? ':' + n.spec.pod.port : ''}${n.spec?.relay ? ':' + RelayProtocol[n.spec.relay.protocol] : ''}`).sort();
const members = after.nodes
	.filter(n => !n.lane && (n.spec?.loadBalanceDistribute || n.spec?.loadBalanceAggregate))
	.map(n => `${n.name}: ${(n.spec?.loadBalanceDistribute?.members ?? n.spec?.loadBalanceAggregate?.members ?? []).map(m => `${m.slot}=${m.name}`).join(' ')}`);
console.log(JSON.stringify({ members, lanes, problems: (await orch.validateCanvas({ canvasId: canvasId! }, opts)).problems.map(p => p.message) }, null, 1));
process.exit(0);
