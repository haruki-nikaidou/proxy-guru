//! Role-based access control: the permission model and capability matrix.
//!
//! Each [`AccountRole`](crate::entities::db::account::AccountRole) maps to
//! a fixed set of [`Permission`]s. The mapping is the single source of truth for
//! "who may do what" and is consulted by the services before any privileged
//! operation.

use crate::entities::db::account::AccountRole;

/// A discrete capability that a role may or may not hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    /// Create, list, re-role, and delete accounts.
    ManageAccounts,
    /// Create, list, and revoke API keys.
    ManageApiKeys,
    /// Read canvases and settings.
    ViewWorkspace,
    /// Create and edit canvases and settings.
    EditWorkspace,
    /// Read and replace installation configuration.
    ManageConfig,
    /// Machine-to-master calls made by a `guru-worker` (registration).
    ServerCall,
    /// Run shell commands on a worker host through the dashboard, on workers
    /// whose host opted in to remote shell.
    RemoteShell,
}

impl AccountRole {
    /// Whether this role is granted `permission`.
    ///
    /// - `Admin` holds every permission (including account management, the
    ///   installation configuration and the remote shell).
    /// - `Maintainer` may view/edit the workspace and manage API keys, but not
    ///   accounts and not the configuration every process in the fleet runs on.
    /// - `Observer` may only view the workspace; notably it cannot manage API
    ///   keys, which is what "Observers may not use API keys" reduces to.
    pub fn can(self, permission: Permission) -> bool {
        match self {
            AccountRole::Admin => true,
            AccountRole::Maintainer => matches!(
                permission,
                Permission::ViewWorkspace
                    | Permission::EditWorkspace
                    | Permission::ManageApiKeys
                    | Permission::ServerCall
            ),
            AccountRole::Observer => matches!(permission, Permission::ViewWorkspace),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Permission; 7] = [
        Permission::ManageAccounts,
        Permission::ManageApiKeys,
        Permission::ViewWorkspace,
        Permission::EditWorkspace,
        Permission::ManageConfig,
        Permission::ServerCall,
        Permission::RemoteShell,
    ];

    fn held(role: AccountRole) -> Vec<Permission> {
        ALL.into_iter().filter(|p| role.can(*p)).collect()
    }

    /// The whole matrix, row by row: a new permission has to be placed here
    /// deliberately, and the remote shell belongs to the Admin alone.
    #[test]
    fn every_role_holds_exactly_its_row() {
        assert_eq!(held(AccountRole::Admin), ALL.to_vec());
        assert_eq!(
            held(AccountRole::Maintainer),
            vec![
                Permission::ManageApiKeys,
                Permission::ViewWorkspace,
                Permission::EditWorkspace,
                Permission::ServerCall,
            ]
        );
        assert_eq!(held(AccountRole::Observer), vec![Permission::ViewWorkspace]);
    }
}
