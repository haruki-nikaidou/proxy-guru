// End-to-end fixture through the operator API: one ingress server with two
// SOCKS ingress pods, a load-balance distribute node whose members (named after
// the transit servers) are bundled to those servers' universal pods, each of
// which is bundled into the same-named member of a load-balance aggregate node,
// two exits.
// Usage: bun scripts/e2e-universal.ts <email> <password> <ingress-name> <exit host:port> <transit-name>...
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import {
	Ipv6Resolve,
	LoadBalanceMode,
	OrchestrationDefinition,
	RelayProtocol,
	UniversalGroup
} from 'app-protobuf/orchestration/orchestration';

const [email, password, ingressName, exitDestination, ...transitNames] = process.argv.slice(2);
if (!email || !password || !ingressName || !exitDestination || transitNames.length === 0) {
	console.error('usage: e2e-universal.ts <email> <password> <ingress> <exit host:port> <transit>...');
	process.exit(2);
}
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);

const login = await auth.login({ email, password, userAgent: 'e2e-universal' });
if (login.result !== LoginResult.SUCCESS) throw new Error(`login failed: ${login.result}`);
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };

const key = await auth.createApiKey({ name: `e2e-${Date.now()}` }, opts);
const canvas = (await orch.createCanvas({ name: `universal ${new Date().toISOString()}`, description: 'universal-node e2e' }, opts)).canvas!;

const server = async (name: string, x: number, y: number) =>
	(await orch.createServer(
		{ canvasId: canvas.id, name, icon: '', comment: '', position: { x: BigInt(x), y: BigInt(y) }, ipv6Resolve: Ipv6Resolve.IPV6_TOLERATED, logLevel: 'info', overrideV4: '', overrideV6: '', extraAddresses: [] },
		opts
	)).server!;
const node = async (name: string, spec: object, x: number, y: number) =>
	(await orch.createNode({ canvasId: canvas.id, name, comment: '', spec, position: { x: BigInt(x), y: BigInt(y) }, itemCount: 0 }, opts)).node!;
const port = (n: { ports: { key: string; id: string }[] }, key: string) => {
	const p = n.ports.find(p => p.key === key);
	if (!p) throw new Error(`no port ${key}`);
	return p.id;
};
const connect = (outputPortId: string, inputPortId: string) => orch.connectPorts({ outputPortId, inputPortId }, opts);
const handle = (nodeId: string, group: UniversalGroup) => ({ nodeId, group });
// The operator's rule: one member per transit server, slot i+1, named after it.
const members = transitNames.map((name, i) => ({ slot: i + 1, name }));

const ingress = await server(ingressName, 0, 0);
const transits = [];
for (const [i, name] of transitNames.entries()) transits.push(await server(name, 900, i * 260));
const universalPodOf = async (serverId: string) => {
	const detail = await orch.getCanvas({ canvasId: canvas.id }, opts);
	const up = detail.nodes.find(n => n.spec?.universalPod?.serverId === serverId);
	if (!up) throw new Error(`server ${serverId} has no universal pod`);
	return up;
};

const ud = await node('fan-out', { loadBalanceDistribute: { mode: LoadBalanceMode.ROUND_ROBIN, protocol: RelayProtocol.RELAY_TCP_RAW, members } }, 500, 100);
const ua = await node('join', { loadBalanceAggregate: { members } }, 1350, 100);
const pods = [];
for (const [i, p] of [10000, 10001].entries()) {
	const pod = await node(`ingress-${p}`, { pod: { serverId: ingress.id, port: p, bindIp: '', advertiseIp: '' } }, 0, 0);
	const entry = await node(`entry-${p}`, { entry: { tls: undefined } }, -350, i * 160);
	await connect(port(pod, 'listen'), port(entry, 'listen'));
	await orch.connectPorts({ outputPortId: '', inputPortId: port(pod, 'destination'), outputHandle: handle(ud.id, UniversalGroup.CHANNEL_OUT) }, opts);
	pods.push(pod);
}
for (const [i, t] of transits.entries()) {
	const up = await universalPodOf(t.id);
	// Member i → the server's "+ bundle" handle; its bundle out → the same member of the aggregate node.
	await orch.connectPorts({ outputPortId: port(ud, `member_${i + 1}`), inputPortId: '', inputHandle: handle(up.id, UniversalGroup.BUNDLE_IN) }, opts);
	await orch.connectPorts({ outputPortId: port(up, 'bundle_out'), inputPortId: port(ua, `member_${i + 1}`) }, opts);
}
const detail = await orch.getCanvas({ canvasId: canvas.id }, opts);
const uaNow = detail.nodes.find(n => n.id === ua.id)!;
const exits = [];
for (const [i, pod] of pods.entries()) {
	const exit = await node(`exit-${i}`, { exit: { destination: exitDestination } }, 1700, i * 160);
	await connect(port(exit, 'destination'), port(uaNow, `chan:${pod.id}`));
	exits.push(exit);
}
const problems = (await orch.validateCanvas({ canvasId: canvas.id }, opts)).problems;
console.log(JSON.stringify({
	canvasId: canvas.id,
	apiKey: key.secret,
	ingress: { id: ingress.id, name: ingress.name, ports: [10000, 10001] },
	transits: transits.map(t => ({ id: t.id, name: t.name })),
	problems: problems.map(p => p.message)
}, null, 2));
process.exit(0);
