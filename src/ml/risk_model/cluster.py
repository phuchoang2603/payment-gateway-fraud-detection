"""Names shared by the steps: the service account, the model name and the namespace."""

import os

SERVICE_ACCOUNT = "fraud-training"
MODEL_NAME = "payment-risk"


def namespace():
    """Returns the namespace the training resources live in, `ML_NAMESPACE` or `ml`."""
    return os.environ.get("ML_NAMESPACE", "ml")
