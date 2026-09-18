import { AuthDefinition } from 'app-protobuf/auth/auth';
import { NotifyDefinition } from 'app-protobuf/notify/notify';
import { OrchestrationDefinition } from 'app-protobuf/orchestration/orchestration';
import { ChannelCredentials, createChannel, createClient } from 'nice-grpc';
import { GURU_GRPC_URL } from '$app/env/private';

/**
 * One process-wide channel to `bin/guru-master`'s dashboard gRPC worker.
 * It serves plain h2c: no TLS, no grpc-web.
 */
const channel = createChannel(GURU_GRPC_URL, ChannelCredentials.createInsecure());

/** Clients are thin proxies over the shared channel; create them per call. */
export const authClient = () => createClient(AuthDefinition, channel);
export const orchestrationClient = () => createClient(OrchestrationDefinition, channel);
export const notifyClient = () => createClient(NotifyDefinition, channel);
