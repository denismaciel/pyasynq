{
  description = "pyasynq: asynq-compatible task queue for Python, powered by Rust";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

  outputs =
    { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python313;
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              # rust
              cargo
              rustc
              rustfmt
              clippy
              rust-analyzer
              maturin

              # python
              python
              uv

              # go asynq, for cross-language tests
              go
              gopls

              # infra
              redis
              postgresql_17
              protobuf
              pkg-config
            ];

            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
            # `make test-ft`: not in `packages`, it would shadow `python3`.
            FREETHREADED_PYTHON = "${pkgs.python314FreeThreading}/bin/python3.14t";
            LIBPQ_DIR = "${lib.getLib pkgs.postgresql_17}/lib";
            UV_PYTHON_PREFERENCE = "only-system";
            UV_PYTHON_DOWNLOADS = "never";
          };
        }
      );
    };
}
