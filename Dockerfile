# One image for all TrustMesh Python services; each service just runs a different command.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY services/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY common ./common
COPY services ./services

RUN useradd --create-home --uid 10001 app
USER app
