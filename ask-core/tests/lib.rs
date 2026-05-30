use ask_core::{WORKSPACE_NAME, workspace_members};

#[test]
fn workspace_name_matches_project_name() {
    assert_eq!(WORKSPACE_NAME, "ask");
}

#[test]
fn workspace_members_include_server() {
    let members = workspace_members();

    assert!(
        members
            .iter()
            .any(|member| member.package_name == "ask-server")
    );
}
