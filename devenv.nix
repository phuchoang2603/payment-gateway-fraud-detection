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
