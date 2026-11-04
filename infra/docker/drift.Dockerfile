FROM python:3.12.16-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.162.0 uvicorn==0.54.0 numpy==2.5.4
COPY src/ml/risk_model/__init__.py src/ml/risk_model/drift.py src/ml/risk_model/drift_service.py src/ml/risk_model/drift_schemas.py ./risk_model/
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["uvicorn", "--factory", "risk_model.drift_service:app", "--host", "0.0.0.0", "--port", "8080"]
