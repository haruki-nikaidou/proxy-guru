import type { RoleName } from '#lib/dto/identity.js';

/** Mirrors `modules/auth/src/utils/rbac.rs`; the backend stays authoritative. */
export const canEditWorkspace = (role: RoleName) => role === 'admin' || role === 'maintainer';
export const canManageApiKeys = (role: RoleName) => role === 'admin' || role === 'maintainer';
export const canManageAccounts = (role: RoleName) => role === 'admin';
/** DNS providers and certificates are Admin only in `modules/orchestration`. */
export const canManageTls = (role: RoleName) => role === 'admin';
/**
 * `Permission::ManageConfig` is Admin-only *and* session-only: an API-key
 * credential is refused even for an Admin, which a browser session never is.
 */
export const canManageConfig = (role: RoleName) => role === 'admin';
