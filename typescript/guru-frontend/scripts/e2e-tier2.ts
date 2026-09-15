// Extends the universal e2e picture: a third ingress pod drawn straight into
// the first transit server, and the second transit server re-routed through a
// second-tier distribute node onto the last two. Usage:
// bun scripts/e2e-tier2.ts <email> <password> <canvasId> <exit host:port>
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { LoadBalanceMode, OrchestrationDefinition, RelayProtocol, UniversalGroup } from 'app-protobuf/orchestration/orchestration';
const [email, password, canvasId, exitDestination] = process.argv.slice(2);
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({ email: email!, password: password!, userAgent: 'e2e-tier2' });
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const canvas = () => orch.getCanvas({ canvasId: canvasId! }, opts);
const handle = (nodeId: string, group: UniversalGroup) => ({ nodeId, group });
const bundle = (from: string, to: string) =>
	orch.connectPorts({ outputPortId: '', inputPortId: '', outputHandle: handle(from, UniversalGroup.BUNDLE_OUT), inputHandle: handle(to, UniversalGroup.BUNDLE_IN) }, opts);
const port = (n: { ports: { key: string; id: string }[] }, key: string) => n.ports.find(p => p.key === key)!.id;

let detail = await canvas();
const server = (name: string) => detail.servers.find(s => s.name === name)!;
const upOf = (name: string) => detail.nodes.find(n => n.spec?.universalPod?.serverId === server(name).id)!;
const byName = (name: string) => detail.nodes.find(n => n.name === name)!;
const us = server('us-ingress');
const ingress = detail.nodes.find(n => n.spec?.pod?.serverId === us.id && n.name.startsWith('ingress-'))!;
const y = Number(ingress.position?.y ?? 0n);

// 1. Thin line: ingress-10002 straight into hk-1.
const p2 = (await orch.createNode({ canvasId: canvasId!, name: 'ingress-10002', comment: '', spec: { pod: { serverId: us.id, port: 10002, bindIp: '', advertiseIp: '' } }, position: { x: 0n, y: 0n }, itemCount: 0 }, opts)).node!;
const entry = (await orch.createNode({ canvasId: canvasId!, name: 'entry-10002', comment: '', spec: { entry: { tls: undefined } }, position: { x: -350n, y: BigInt(y + 320) }, itemCount: 0 }, opts)).node!;
await orch.connectPorts({ outputPortId: port(p2, 'listen'), inputPortId: port(entry, 'listen') }, opts);
await orch.connectPorts({ outputPortId: '', inputPortId: port(p2, 'destination'), outputHandle: handle(upOf('hk-1').id, UniversalGroup.CHANNEL_OUT) }, opts);

// 2. Second tier: hk-2 -> tier-2 (round robin, TCP) -> hk-3, hk-4.
detail = await canvas();
const hk2out = detail.edges.find(e => upOf('hk-2').ports.some(p => p.id === e.sourcePortId))!;
await orch.disconnect({ edgeId: hk2out.id }, opts);
const ud2 = (await orch.createNode({ canvasId: canvasId!, name: 'tier-2', comment: '', spec: { loadBalanceDistribute: { mode: LoadBalanceMode.ROUND_ROBIN, protocol: RelayProtocol.RELAY_TCP_RAW } }, position: { x: 1350n, y: BigInt(y + 380) }, itemCount: 0 }, opts)).node!;
await bundle(upOf('hk-2').id, ud2.id);
await bundle(ud2.id, upOf('hk-3').id);
await bundle(ud2.id, upOf('hk-4').id);

// 3. The new channel needs an exit on the aggregate node.
detail = await canvas();
const join = byName('join');
const exit2 = (await orch.createNode({ canvasId: canvasId!, name: 'exit-2', comment: '', spec: { exit: { destination: exitDestination! } }, position: { x: 1700n, y: BigInt(y + 320) }, itemCount: 0 }, opts)).node!;
await orch.connectPorts({ outputPortId: port(exit2, 'destination'), inputPortId: port(join, `chan:${p2.id}`) }, opts);
const problems = (await orch.validateCanvas({ canvasId: canvasId! }, opts)).problems.map(p => p.message);
detail = await canvas();
const lanes = detail.nodes.filter(n => n.lane).length;
console.log(JSON.stringify({ lanes, problems }, null, 1));
process.exit(0);
