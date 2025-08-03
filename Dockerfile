FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

RUN pip install poetry

COPY . .

EXPOSE 8000

RUN poetry config virtualenvs.create false

RUN poetry install --no-root --no-interaction --no-ansi

ENV PYTHONPATH=/app
ENV PYTHONBUFFERED=1
