{
  description = "infretis on HPC via HyperQueue (GUTS Fast Track)";

  # Same nixpkgs revision as ../hyperqueue_test (flake.lock copied from there).
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # Keep in sync with HQ_VERSION / HQ_SHA256 in scripts/get-hq.sh, so the
      # laptop and the clusters run the same binary.
      hq = pkgs.stdenv.mkDerivation rec {
        pname = "hyperqueue";
        version = "0.26.2";
        src = pkgs.fetchurl {
          url = "https://github.com/It4innovations/hyperqueue/releases/download/v${version}/hq-v${version}-linux-x64.tar.gz";
          hash = "sha256-4V2ukRPhowepema/6Q9094xgFiOUNrXZ8eTv7EgOhLU=";
        };
        sourceRoot = ".";
        nativeBuildInputs = [ pkgs.autoPatchelfHook ];
        buildInputs = [ pkgs.stdenv.cc.cc.lib ];
        installPhase = "install -Dm755 hq $out/bin/hq";
      };
    in
    {
      packages.${system} = { inherit hq; default = hq; };

      # Python packages come from pip into .venv (scripts/setup-venv.sh),
      # because infretis pins numpy < 2 and nixpkgs ships numpy 2.
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          hq
          pkgs.python312
          pkgs.gromacs   # water_boiling, puckering
          pkgs.lammps    # water_dissociation; built with REAXFF + QEQ
          pkgs.ruff      # lint (pyproject.toml [tool.ruff])
          pkgs.pyright   # type check (pyproject.toml [tool.pyright])
        ];
        # Let pip wheels (numpy, mdtraj, the hyperqueue client) find libstdc++
        # and zlib on NixOS.
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ];
      };
    };
}
