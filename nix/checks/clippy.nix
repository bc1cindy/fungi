{
  perSystem =
    {
      cargoWorkspaces,
      pkgs,
      toolchains,
      ...
    }:
    let
      workspace = import ./workspace.nix { inherit cargoWorkspaces pkgs; };
    in
    {
      workspaceChecks =
        workspace.mapWorkspaces
          {
            name = "clippy";
            tags = [
              "nightly"
              "lint"
              "quick"
            ];
          }
          (
            _: cargoWorkspace:
            toolchains.nightly.cargoClippy (
              (workspace.checkArgs cargoWorkspace)
              // {
                cargoArtifacts = cargoWorkspace.cargoArtifactsDev;
                cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings -D unused";
              }
            )
          );
    };
}
