{
  perSystem =
    { cargoWorkspaces, pkgs, ... }:
    let
      workspace = import ./workspace.nix { inherit cargoWorkspaces pkgs; };
    in
    {
      workspaceChecks =
        workspace.mapWorkspaces
          {
            name = "cargo-sort";
            tags = [
              "nightly"
              "lint"
            ];
          }
          (
            workspaceName: cargoWorkspace:
            pkgs.runCommand "${workspaceName}-cargo-sort"
              {
                inherit (cargoWorkspace.commonArgs) src;
                nativeBuildInputs = [ pkgs.cargo-sort ];
              }
              ''
                cargo-sort --check --workspace "$src/${builtins.dirOf cargoWorkspace.manifestPath}"
                mkdir -p "$out"
              ''
          );
    };
}
