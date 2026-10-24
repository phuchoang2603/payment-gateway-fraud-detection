# Payment Gateway and Real-Time Fraud Detection

This project builds an ecommerce payment gateway with fraud decisioning. The planned gateway owns hosted checkout, payment execution and risk decisions; the marketplace keeps ownership of customers, sellers, orders and inventory. Initial processing uses synthetic payment-method tokens and a processor simulator.

The repository implements six Go services, including the independent fraud decision service at `src/payment-gateway/fraud` with a stateless rules baseline. The [Go fraud and observability change](openspec/changes/go-service-observability/proposal.md) adds correlated telemetry; CI and deployed acceptance remain pending. Later, Go fraud calls Feast feature serving and KServe inference directly; Python remains in training, feature transformations and model runtimes where needed. Data/ML pipelines and production acceptance have their own milestones. Source/configuration presence does not establish a live deployment or completed coursework.

## Repository structure

```text
.
├── .github/workflows/      # Independent check jobs, image build workflow, wiki publication
├── CONTRIBUTING.md         # Initial setup and wiki link
├── contracts/              # Versioned protobuf service contracts
├── docs/
│   ├── architecture/      # Gateway overview, payment and data/ML flows, conventions, roadmap
│   ├── deployment/        # GitOps, runtime configuration and observability
│   ├── verification/      # Acceptance criteria and verification guidance
│   ├── data-products/     # Source interfaces and data contracts
│   └── research/          # Background, experiments and images
├── infra/
│   ├── argocd/            # Application catalog and prod root
│   ├── charts/            # Application Helm charts
│   └── docker/            # Service Dockerfiles and reusable Go template
├── openspec/              # Capability specifications and change plans
├── src/payment-gateway/   # Go gateway services, shared module and workspace
├── src/simulator/         # Historical data simulator
├── devenv.nix             # Local development environment
├── devenv.yaml            # Devenv inputs
└── devenv.lock            # Locked environment dependencies
```

## Documentation

Browse the [project wiki](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki) for architecture, the implementation roadmap, deployment and verification guidance; see [CONTRIBUTING](CONTRIBUTING.md) to work in the repository.
