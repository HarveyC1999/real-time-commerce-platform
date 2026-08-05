FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /workspace

COPY pyproject.toml ./
COPY app ./app
COPY scripts ./scripts

RUN python -m pip install --upgrade pip \
    && python -m pip install .

RUN useradd \
    --create-home \
    --shell /usr/sbin/nologin \
    appuser

USER appuser

CMD ["python", "scripts/consume_orders.py"]