# Use the official Python base image
FROM --platform=linux/amd64 python:3.11-rc-slim-buster as pythonbaseimage

RUN pip install poetry


ENV PATH="/root/.poetry/bin:${PATH}"

COPY ./xtdb_juxt ./xtdb_juxt
COPY ./pyproject.toml .
COPY ./README.md .

RUN poetry install


ENTRYPOINT ["poetry", "run", "pytest", "xtdb_juxt/test"]