{
  perSystem =
    { config, lib, ... }:
    {
      options.workspaceChecks = lib.mkOption {
        default = { };
        description = "Reusable checks registered by workspace and check name.";
        type = lib.types.attrsOf (
          lib.types.attrsOf (
            lib.types.submodule {
              options = {
                package = lib.mkOption {
                  type = lib.types.package;
                  description = "Derivation that runs the check.";
                };
                tags = lib.mkOption {
                  type = lib.types.listOf (
                    lib.types.enum [
                      "lint"
                      "nightly"
                      "quick"
                    ]
                  );
                  default = [ ];
                  description = "Aggregate groups that include this check.";
                };
              };
            }
          )
        );
      };

      config.checks = lib.concatMapAttrs (
        workspaceName: checks:
        lib.mapAttrs' (name: check: lib.nameValuePair "${workspaceName}-${name}" check.package) checks
      ) config.workspaceChecks;
    };
}
