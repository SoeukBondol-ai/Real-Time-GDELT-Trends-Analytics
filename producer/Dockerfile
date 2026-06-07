FROM ghcr.io/astral-sh/uv:0.4.10 AS uv
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app

RUN uv pip install --system --no-cache .

CMD ["python", "-m", "app.main"]
