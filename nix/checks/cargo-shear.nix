{
  perSystem =
    {
      cargoWorkspaces,
      pkgs,
      ...
    }:
    let
      workspace = import ./workspace.nix { inherit cargoWorkspaces pkgs; };
    in
    {
      workspaceChecks =
        workspace.mapWorkspaces
          {
            name = "cargo-shear";
            tags = [
              "nightly"
              "lint"
            ];
          }
          (
            workspaceName: cargoWorkspace:
            pkgs.runCommand "${workspaceName}-cargo-shear"
              {
                inherit (cargoWorkspace.commonArgs) cargoVendorDir src;
                nativeBuildInputs = [
                  pkgs.cargo
                  pkgs.cargo-shear
                ];
              }
              ''
                export CARGO_HOME="$TMPDIR/cargo-home"
                export CARGO_NET_OFFLINE=true
                mkdir -p "$CARGO_HOME"
                cp "$cargoVendorDir/config.toml" "$CARGO_HOME/config.toml"
                cd "$src/${builtins.dirOf cargoWorkspace.manifestPath}"
                cargo-shear
                mkdir -p "$out"
              ''
          );
    };
}
