// Draws the operator's rule on an existing canvas: the distribute node gets one
// member per server (named after it) bundled to that server's universal pod,
// and every universal pod is bundled into the same-named member of the
// aggregate node. Members already present keep their slots; bundles already
// drawn are left alone. Usage:
// bun scripts/e2e-members.ts <email> <password> <canvasId> <distribute-name> <aggregate-name> <server-name>...
import { ChannelCredentials, createChannel, createClient, Metadata } from 'nice-grpc';
import { AuthDefinition, LoginResult } from 'app-protobuf/auth/auth';
import { OrchestrationDefinition, UniversalGroup } from 'app-protobuf/orchestration/orchestration';
const [email, password, canvasId, distributeName, aggregateName, ...serverNames] = process.argv.slice(2);
if (!canvasId || !distributeName || !aggregateName || serverNames.length === 0) {
	console.error('usage: e2e-members.ts <email> <password> <canvasId> <distribute> <aggregate> <server>...');
	process.exit(2);
}
const channel = createChannel('127.0.0.1:50051', ChannelCredentials.createInsecure());
const auth = createClient(AuthDefinition, channel);
const orch = createClient(OrchestrationDefinition, channel);
const login = await auth.login({ email: email!, password: password!, userAgent: 'e2e-members' });
if (login.result !== LoginResult.SUCCESS) throw new Error('login failed');
const opts = { metadata: new Metadata({ 'x-session-id': login.sessionId }) };
const canvas = () => orch.getCanvas({ canvasId }, opts);
const handle = (nodeId: string, group: UniversalGroup) => ({ nodeId, group });
const port = (n: { ports: { key: string; id: string }[] }, key: string) => {
	const p = n.ports.find(p => p.key === key);
	if (!p) throw new Error(`no port ${key}`);
	return p.id;
};

let detail = await canvas();
const byName = (name: string) => {
	const node = detail.nodes.find(n => n.name === name && !n.lane);
	if (!node) throw new Error(`no node ${name}`);
	return node;
};
const upOf = (name: string) => {
	const server = detail.servers.find(s => s.name === name);
	const up = server && detail.nodes.find(n => n.spec?.universalPod?.serverId === server.id);
	if (!up) throw new Error(`no universal pod for server ${name}`);
	return up;
};
const wired = (portId: string) => detail.edges.some(e => e.sourcePortId === portId || e.targetPortId === portId);

// 1. The member lists: keep known names on their slots, append the rest.
const withMembers = async (name: string) => {
	const node = byName(name);
	const current = node.spec?.loadBalanceDistribute?.members ?? node.spec?.loadBalanceAggregate?.members ?? [];
	let next = current.reduce((max, m) => Math.max(max, m.slot), 0);
	const members = serverNames.map(server => current.find(m => m.name === server) ?? { slot: ++next, name: server });
	const spec = node.spec?.loadBalanceDistribute
		? { loadBalanceDistribute: { ...node.spec.loadBalanceDistribute, members } }
		: { loadBalanceAggregate: { members } };
	await orch.replaceNodeSpec({ nodeId: node.id, spec, itemCount: 0 }, opts);
	return members;
};
const outMembers = await withMembers(distributeName);
const inMembers = await withMembers(aggregateName);

// 2. The bundles: member → server's "+ bundle"; server's bundle out → member.
for (const server of serverNames) {
	detail = await canvas();
	const ud = byName(distributeName);
	const ua = byName(aggregateName);
	const up = upOf(server);
	const out = port(ud, `member_${outMembers.find(m => m.name === server)!.slot}`);
	if (!wired(out)) {
		await orch.connectPorts({ outputPortId: out, inputPortId: '', inputHandle: handle(up.id, UniversalGroup.BUNDLE_IN) }, opts);
	}
	detail = await canvas();
	const upNow = upOf(server);
	const upOut = port(upNow, 'bundle_out');
	const into = port(ua, `member_${inMembers.find(m => m.name === server)!.slot}`);
	if (!wired(upOut) && !wired(into)) {
		await orch.connectPorts({ outputPortId: upOut, inputPortId: into }, opts);
	}
}
detail = await canvas();
const lanes = detail.nodes.filter(n => n.lane).length;
const problems = (await orch.validateCanvas({ canvasId }, opts)).problems.map(p => p.message);
console.log(JSON.stringify({ out: outMembers, in: inMembers, lanes, problems }, null, 1));
process.exit(0);
