FROM openmetadata/ingestion:2.0.6@sha256:fd104fda383aa86750d89af8787d6e1f5b8f3aa8c606fee08eb4c616fcf5df85
COPY src/data-pipeline/bronze-ingestion/data_platform /opt/data-platform/data_platform
ENV PYTHONPATH=/opt/data-platform \
    PYTHONDONTWRITEBYTECODE=1
RUN python -c 'import data_platform.integrations'
ENTRYPOINT ["python"]
