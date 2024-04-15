# Use the official Python base image
FROM --platform=linux/amd64 python:3.11-rc-slim-buster as pythonbaseimage

RUN pip install poetry

ENV PATH="/root/.poetry/bin:${PATH}"


COPY ./pyproject.toml .
COPY ./poetry.lock .
RUN poetry install


COPY ./xtdb_juxt ./xtdb_juxt


ENTRYPOINT ["poetry", "run", "pytest", "xtdb_juxt/test"]
