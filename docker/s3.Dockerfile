# Local S3-compatible endpoint for development and CI, built from PyPI (moto server).
# MinIO's community images were removed from Docker Hub; moto has no such dependency.
# Data is kept in memory: it lasts for the life of the container. Use real S3 for persistence.
FROM python:3.12-slim
RUN pip install --no-cache-dir "moto[server]==5.2.5"
EXPOSE 9000
HEALTHCHECK --interval=3s --timeout=3s --retries=20 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9000/moto-api/')"
CMD ["moto_server", "-H", "0.0.0.0", "-p", "9000"]
