FROM ghcr.io/astral-sh/uv:0.4.10 AS uv
FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=uv /uv /usr/local/bin/uv

USER airflow
COPY airflow/pyproject.toml /tmp/pyproject.toml
RUN uv pip compile /tmp/pyproject.toml -o /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt
