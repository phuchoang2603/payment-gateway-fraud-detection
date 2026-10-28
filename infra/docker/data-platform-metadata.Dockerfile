FROM openmetadata/ingestion:2.0.7@sha256:5c0bd8728d4f92b67de3fac1a5664015364b7eca07d0038504138f17a3d243c6
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
