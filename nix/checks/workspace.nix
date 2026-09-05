{ cargoWorkspaces, pkgs }:
{
  checkArgs =
    workspace:
    workspace.commonArgs
    // {
      dontFixup = true;
      doInstallCargoArtifacts = false;
      CARGO_PROFILE = "";
    };

  mapWorkspaces =
    {
      name,
      tags ? [ ],
    }:
    mkCheck:
    pkgs.lib.mapAttrs (workspaceName: workspace: {
      ${name} = {
        inherit tags;
        package = mkCheck workspaceName workspace;
      };
    }) cargoWorkspaces;
}
