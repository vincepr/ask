/// The shared workspace name.
pub const WORKSPACE_NAME: &str = "ask";

pub mod models;
pub mod repository;
pub mod schema;
pub mod types;

/// Describes a workspace member and its responsibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceMember {
    /// The Cargo package name.
    pub package_name: &'static str,
    /// The primary responsibility of the package.
    pub role: &'static str,
}

const MEMBERS: [WorkspaceMember; 2] = [
    WorkspaceMember {
        package_name: "ask-core",
        role: "shared domain logic and reusable primitives",
    },
    WorkspaceMember {
        package_name: "ask-server",
        role: "service runtime, API surface, and background work",
    },
];

/// Returns the workspace members declared for the initial project layout.
#[must_use]
pub fn workspace_members() -> &'static [WorkspaceMember] {
    &MEMBERS
}
