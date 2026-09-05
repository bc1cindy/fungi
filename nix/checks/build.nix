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
            name = "build";
            tags = [ "nightly" ];
          }
          (
            _: cargoWorkspace:
            toolchains.nightly.buildPackage (
              (workspace.checkArgs cargoWorkspace)
              // {
                cargoArtifacts = cargoWorkspace.cargoArtifactsDev;
                # The tests-* checks run the test suite under nextest.
                doCheck = false;
              }
            )
          );
    };
}
