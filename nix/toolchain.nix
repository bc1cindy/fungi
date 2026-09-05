{ inputs, ... }:
{
  perSystem =
    { system, ... }:
    let
      pkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [ inputs.rust-overlay.overlays.default ];
      };

      commonExtensions = [
        "clippy"
        "rust-analyzer"
        "rust-src"
      ];

      rustToolchains = {
        nightly = pkgs.rust-bin.selectLatestNightlyWith (
          t:
          t.default.override {
            extensions = commonExtensions ++ [ "llvm-tools" ];
          }
        );
        beta = pkgs.rust-bin.beta.latest.default.override {
          extensions = commonExtensions;
        };
        stable = pkgs.rust-bin.stable.latest.default.override {
          extensions = commonExtensions;
        };
      };

      mkCraneLib = _: rust: (inputs.crane.mkLib pkgs).overrideToolchain rust;
      toolchains = builtins.mapAttrs mkCraneLib rustToolchains;
    in
    {
      _module.args = {
        inherit pkgs toolchains;
      };
    };
}
