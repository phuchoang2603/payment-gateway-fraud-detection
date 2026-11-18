{
  pkgs,
  config,
  inputs,
  ...
}:
let
  module = "github.com/phuchoang2603/payment-gateway-fraud-detection";
  denyDomain =
    desc: packages:
    map (pkg: {
      inherit pkg desc;
    }) packages;
in
{
  imports = map (m: "${inputs.devenv-modules}/${m}.nix") [
    "base"
    "go"
    "sqlc"
    "proto"
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

    openspec
    zlib
  ];

  repo.gremlins.enable = true;

  repo.golangci = {
    linters.enable = [ "depguard" ];
    linters.exclusions.generated = "strict";
    linters.settings.depguard.rules.domain = {
      files = [ "**/internal/domain/**" ];
      deny =
        denyDomain "Domain code must not depend on transport." [
          "net/http"
          "google.golang.org/grpc"
        ]
        ++ denyDomain "Domain code must not depend on generated protobuf bindings." [
          "${module}/shared/proto"
          "google.golang.org/protobuf"
        ]
        ++ denyDomain "Domain code must not depend on databases or storage adapters." [
          "database/sql"
          "github.com/jackc/pgx"
          "github.com/redis/go-redis"
          "github.com/twmb/franz-go"
        ]
        ++ denyDomain "Domain code must not depend on observability." [
          "go.opentelemetry.io"
          "${module}/shared/observe"
        ];
    };
  };

  tasks."codegen:proto-python" = {
    exec = ''python3 "${config.git.root}/scripts/codegen-proto.py"'';
    after = [ "codegen:proto" ];
    execIfModified = [
      "contracts/**/*.proto"
      "scripts/codegen-proto.py"
      "src/**/pyproject.toml"
      "devenv.lock"
      "devenv.nix"
    ];
  };

  treefmt.config.programs = {
    ruff-format.excludes = [ "src/data-pipeline/simulator/simulator/contracts/**" ];
  };
}
