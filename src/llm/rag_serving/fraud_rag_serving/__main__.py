"""Start the knowledge API HTTP listener with the configured application port."""

import os

import uvicorn

from fraud_rag_serving.app import app

uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), access_log=False)
