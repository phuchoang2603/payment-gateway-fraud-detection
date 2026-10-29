FROM python:3.13-slim-bookworm
RUN pip install --no-cache-dir inference-perf==0.7.0
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 65532:65532
ENTRYPOINT ["inference-perf"]
