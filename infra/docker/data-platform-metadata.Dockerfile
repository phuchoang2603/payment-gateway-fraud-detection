FROM openmetadata/ingestion:2.0.6@sha256:fd104fda383aa86750d89af8787d6e1f5b8f3aa8c606fee08eb4c616fcf5df85
COPY src/data-pipeline/openmetadata_jobs/openmetadata_jobs /opt/data-pipeline/openmetadata_jobs
COPY contracts/data-products /contracts/data-products
ENV PYTHONPATH=/opt/data-pipeline \
    DATA_PRODUCTS_ROOT=/contracts/data-products \
    PYTHONDONTWRITEBYTECODE=1
RUN python -c 'import openmetadata_jobs.integrations'
ENTRYPOINT ["python"]
