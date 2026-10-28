FROM openmetadata/ingestion:2.0.6@sha256:fd104fda383aa86750d89af8787d6e1f5b8f3aa8c606fee08eb4c616fcf5df85
COPY src/data-pipeline/batch/fraud_batch /opt/data-platform/fraud_batch
COPY contracts/data-products /contracts/data-products
COPY contracts/features /contracts/features
COPY contracts/quality /contracts/quality
ENV PYTHONPATH=/opt/data-platform \
    DATA_PRODUCTS_ROOT=/contracts/data-products \
    DATA_QUALITY_ROOT=/contracts/quality \
    PYTHONDONTWRITEBYTECODE=1
RUN python -c 'import boto3, yaml, pymysql; from metadata.ingestion.ometa.ometa_api import OpenMetadata; import fraud_batch.bronze.runtime, fraud_batch.bronze.bronze_metadata, fraud_batch.common.integrations, fraud_batch.silver.silver_native, fraud_batch.silver.silver_governance; import confluent_kafka'
ENTRYPOINT ["python"]
