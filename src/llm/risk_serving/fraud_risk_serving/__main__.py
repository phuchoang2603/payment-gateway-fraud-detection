import os

import uvicorn

uvicorn.run("fraud_risk_serving.app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
