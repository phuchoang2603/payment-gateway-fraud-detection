# Training

**Status: planned.** The training data now exists (v19 full-scale source); the pipeline is built after Silver and DP3.

## Training data

- One row is one scored payment attempt. Features use only information available before the attempt time; labels join on attempt identity and only when `label_available_at` is before the training cutoff.
- Target: simulation truth (`is_fraud`). Evaluation also reports chargebacks available before the cutoff, the only label production would have. An absent chargeback is unknown, not negative.
- Splits are chronological, with a warm-up period and a gap that covers the chargeback delay, not random.
- Background on fraud scenarios, imbalance and evaluation metrics: [background](background.md).

## Pipeline

| Step | Plan |
|---|---|
| Notebook baseline | Load features from the Feast offline store, join the label table, split by time, train a classifier, evaluate with precision, recall and PR-AUC against the rules baseline, save the model |
| Training pipeline | The same steps as a Kubeflow pipeline sharing the notebook's functions; distributed training for the train step |
| Versioning | Model weights, hyperparameters and metrics in the platform model registry; training datasets as Iceberg snapshot references so each version stores only what changed |
| Promotion | Failed evaluation cannot promote; an approved version supplies the artifact URI for KServe |

## Drift

A scheduled job compares current and reference feature distributions without assuming immediate labels, publishes the result to dashboards, and triggers deduplicated, cooldown-controlled retraining. The generator gains a configurable drift schedule for this. Planned.
