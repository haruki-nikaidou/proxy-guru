//! The authenticated principal shared across services and the transport edge.

use crate::entities::db::account::{AccountId, AccountRole};
use crate::utils::rbac::Permission;

/// An authenticated caller: which account, its current role, and how it proved
/// its identity (a human session or a machine API key).
#[derive(Clone, Debug)]
pub struct Identity {
    pub account_id: AccountId,
    pub role: AccountRole,
    pub kind: IdentityKind,
}

/// How an [`Identity`] was established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityKind {
    /// A human logged in with email + password, carrying a session id.
    Session,
    /// A machine presenting an API key secret.
    ApiKey,
}

impl Identity {
    /// Authorize a privileged operation.
    ///
    /// The credential kind is decided per permission: [`Permission::ServerCall`]
    /// is machine-only (a worker registering with an operator API key), every
    /// other permission is human-session-only, so a server credential can never
    /// drive account/key management or edit the workspace. The role's capability
    /// matrix then decides.
    pub fn ensure(&self, permission: Permission) -> Result<(), wakuwaku::Error> {
        let kind_ok = match permission {
            Permission::ServerCall => self.kind == IdentityKind::ApiKey,
            _ => self.kind == IdentityKind::Session,
        };
        if !kind_ok {
            return Err(wakuwaku::Error::PermissionsDenied);
        }
        if self.role.can(permission) {
            Ok(())
        } else {
            Err(wakuwaku::Error::PermissionsDenied)
        }
    }

    /// Require that this identity is a human session (used for self-service
    /// operations like changing one's own password or email).
    pub fn require_human(&self) -> Result<(), wakuwaku::Error> {
        if self.kind == IdentityKind::Session {
            Ok(())
        } else {
            Err(wakuwaku::Error::PermissionsDenied)
        }
    }
}
