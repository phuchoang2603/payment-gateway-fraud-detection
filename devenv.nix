{ pkgs, config, ... }:
{
  packages = with pkgs; [
    kubernetes-helm
    kubectl

    ruff
    gofumpt
    golangci-lint

    protobuf
    protoc-gen-go
    protoc-gen-go-grpc

    sqlc
    goose

    openspec
    zlib
  ];

  languages = {
    go = {
      delve = {
        enable = true;
      };
      enable = true;
      lsp = {
        enable = true;
      };
    };
    python = {
      enable = true;
      lsp = {
        enable = true;
      };
      uv = {
        enable = true;
      };
      version = "3.14";
    };
  };

  tasks."codegen:proto" = {
    exec = ''bash "${config.git.root}/scripts/codegen-proto.sh"'';
    before = [ "devenv:enterShell" ];
    execIfModified = [
      "contracts/**/*.proto"
      "scripts/codegen-proto.sh"
      "devenv.lock"
      "devenv.nix"
    ];
  };

  tasks."codegen:sqlc" = {
    exec = ''bash "${config.git.root}/scripts/codegen-sqlc.sh"'';
    before = [ "devenv:enterShell" ];
    execIfModified = [
      "src/payment-gateway/*/sqlc.yaml"
      "src/payment-gateway/*/db/migrations/*.sql"
      "src/payment-gateway/*/db/queries/*.sql"
      "scripts/codegen-sqlc.sh"
    ];
  };

  git-hooks.hooks = {
    treefmt.enable = true;
  };

  treefmt = {
    enable = true;
    config.programs = {
      ruff-format = {
        enable = true;
      };
      gofumpt = {
        enable = true;
        extra = true;
        excludes = [
          "src/payment-gateway/shared/proto/**/*.pb.go"
        ];
      };
    };
  };
}
