FROM openmetadata/ingestion:2.0.6@sha256:fd104fda383aa86750d89af8787d6e1f5b8f3aa8c606fee08eb4c616fcf5df85
COPY src/data-pipeline/bronze-ingestion/data_platform /opt/data-platform/data_platform
COPY contracts/data-products /contracts/data-products
COPY contracts/features /contracts/features
COPY contracts/quality /contracts/quality
ENV PYTHONPATH=/opt/data-platform \
    DATA_PRODUCTS_ROOT=/contracts/data-products \
    DATA_QUALITY_ROOT=/contracts/quality \
    PYTHONDONTWRITEBYTECODE=1
RUN python -c 'import boto3, yaml, pymysql; from metadata.ingestion.ometa.ometa_api import OpenMetadata; import data_platform.runtime, data_platform.bronze_metadata, data_platform.integrations, data_platform.silver_native, data_platform.silver_governance; import confluent_kafka'
ENTRYPOINT ["python"]
