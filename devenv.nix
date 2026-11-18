{
  pkgs,
  config,
  inputs,
  ...
}:
{
  imports = map (m: "${inputs.devenv-modules}/${m}.nix") [
    "base"
    "go"
    "python"
    "kubernetes"
  ];

  env.JAVA_HOME = "${pkgs.jdk17}/lib/openjdk";
  env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
    pkgs.stdenv.cc.cc
    pkgs.zlib
  ];

  packages = with pkgs; [
    jdk17
    maven

    protobuf
    protoc-gen-go
    protoc-gen-go-grpc

    sqlc
    goose

    openspec
    zlib
  ];

  repo.lintIgnores = [ "infra/charts/ml-training/files/**" ];

  repo.golangci = {
    linters.exclusions.generated = "strict";
    formatters.exclusions.paths = [ "\\.pb\\.go$" ];
  };

  tasks."codegen:proto" = {
    exec = ''bash "${config.git.root}/scripts/codegen-proto.sh"'';
    execIfModified = [
      "contracts/**/*.proto"
      "scripts/codegen-proto.sh"
      "scripts/codegen-proto.py"
      "src/**/pyproject.toml"
      "devenv.lock"
      "devenv.nix"
    ];
  };

  tasks."codegen:sqlc" = {
    exec = ''bash "${config.git.root}/scripts/codegen-sqlc.sh"'';
    execIfModified = [
      "src/payment-gateway/*/sqlc.yaml"
      "src/payment-gateway/*/db/migrations/*.sql"
      "src/payment-gateway/*/db/queries/*.sql"
      "scripts/codegen-sqlc.sh"
    ];
  };

  treefmt.config.programs = {
    ruff-format.excludes = [ "src/data-pipeline/simulator/simulator/contracts/**" ];
    gofumpt.excludes = [ "src/payment-gateway/shared/proto/**/*.pb.go" ];
  };
}
