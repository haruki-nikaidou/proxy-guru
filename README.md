<p align="center">
  <img src="typescript/docs/public/favicon.svg" alt="Proxy Guru" width="120" height="120">
</p>

<h1 align="center">Proxy Guru</h1>

<p align="center">
  A managed TCP/TLS proxy fabric: design a topology on a canvas, ship one config per server.
</p>

<p align="center">
  <a href="https://guru.plr.moe/"><b>Documentation</b></a>
</p>

<p align="center">
  <img src="typescript/docs/public/img/canvas/canvas-overview.avif" alt="The Proxy Guru canvas: client pods on a Singapore server balancing through a relay pod on a US server into the exit example.com">
</p>

Operators draw the forwarding topology on a canvas, the control plane
(`bin/guru-master`) checks the whole graph, derives one config per server and
rolls it out; data-plane workers (`bin/guru-worker`) terminate the listeners and
pick up every new revision automatically — standalone from a TOML file or in
agent mode.

Rust 2024 on Tokio with PostgreSQL, AMQP, Redis and gRPC, plus a Bun workspace
under `typescript/` for the dashboard and the docs.

## Start here

- [Introduction](https://guru.plr.moe/guides/introduction/) — what the pieces are and how traffic flows
- [Local Development](https://guru.plr.moe/guides/local-development/) — the whole stack on one machine
- [Deploy with Docker](https://guru.plr.moe/guides/deploy-with-docker/) — the published images
- [Canvas](https://guru.plr.moe/reference/canvas/) — the pod graph and every edit gesture

Read [`AGENTS.md`](AGENTS.md) before adding code: it is the code-organisation
contract. The docs site itself lives in `typescript/docs` (`bun run docs:dev`).
