# syntax=docker/dockerfile:1.7
#
# `typescript/guru-frontend` — the SvelteKit dashboard (adapter-node).
#
# The context is the repository root, not the package: `guru-frontend` consumes
# the generated gRPC code through the `app-protobuf` workspace package, so the
# whole Bun workspace has to be present at install time.
#
#   docker build -f frontend.Dockerfile -t guru-frontend .

FROM oven/bun:1.3 AS base
WORKDIR /app
ENV CI="true"

# Manifests only: this layer survives every source-only change.
COPY package.json bun.lock ./
COPY typescript/app-protobuf/package.json typescript/app-protobuf/
COPY typescript/guru-frontend/package.json typescript/guru-frontend/

FROM base AS builder
RUN bun install --frozen-lockfile
COPY biome.json ./
COPY proto/ proto/
COPY typescript/ typescript/
ENV NODE_ENV="production"
RUN bun run --filter guru-frontend build

# adapter-node bundles the app but keeps `dependencies` external (`nice-grpc`,
# `valibot`, …), so the runtime still needs a production `node_modules`.
FROM base AS deps
RUN bun install --frozen-lockfile --production --ignore-scripts

FROM gcr.io/distroless/nodejs24-debian13:nonroot AS runtime
# Bun's isolated linker keeps the shared store in `<root>/node_modules/.bun` and
# symlinks each package's deps into `typescript/<pkg>/node_modules`, so the
# workspace tree is reproduced verbatim and the app runs from inside it.
COPY --from=deps /app/node_modules /app/node_modules
COPY --from=deps /app/typescript /app/typescript
COPY --from=builder /app/typescript/guru-frontend/build /app/typescript/guru-frontend/build

# `build/*.js` is ESM; the package manifest above it declares `"type": "module"`.
WORKDIR /app/typescript/guru-frontend

EXPOSE 3000

ENV NODE_ENV="production"
ENV HOST="0.0.0.0"
ENV PORT="3000"
# Dashboard gRPC endpoint of `guru-master --mode dashboard_grpc`.
ENV GURU_GRPC_URL="127.0.0.1:50051"
# Serve it over HTTPS. The app reconstructs its origin per request and assumes
# `https` unless `PROTOCOL_HEADER` is set, rejecting mismatched POSTs with 403;
# behind a plain-HTTP or port-shifted proxy set `PROTOCOL_HEADER=x-forwarded-proto`
# and `HOST_HEADER=x-forwarded-host`. Runtime `ORIGIN` is ignored: adapter-node
# bakes `kit.paths.origin` in at build time.

CMD ["build/index.js"]
