{
  description = "infretis on HPC via HyperQueue (GUTS Fast Track)";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # The same release as on the clusters (scripts/get-hq.sh).
      release = builtins.fromTOML (builtins.readFile ./src/gutsrun/hq-release.toml);
      hq = pkgs.stdenv.mkDerivation rec {
        pname = "hyperqueue";
        inherit (release) version;
        src = pkgs.fetchurl {
          url = "https://github.com/It4innovations/hyperqueue/releases/download/v${version}/hq-v${version}-linux-x64.tar.gz";
          inherit (release) sha256;
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
          pkgs.jq        # scripts/wait-jobs.sh --run reads `status --json`
        ];
        # Let pip wheels (numpy, mdtraj) find libstdc++
        # and zlib on NixOS.
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ];
      };
    };
}
