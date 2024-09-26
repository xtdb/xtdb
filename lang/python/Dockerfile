FROM python:latest

WORKDIR /app

ENTRYPOINT ["poetry"]
CMD ["run", "pytest"]

RUN pip install poetry

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install

COPY . .
