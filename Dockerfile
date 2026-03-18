FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY ./ ./
RUN uv sync --no-dev && rm -rf /root/.cache/uv

EXPOSE 8000
USER nobody

CMD [ ".venv/bin/python", "main.py" ]
