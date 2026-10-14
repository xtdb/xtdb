FROM python:3.15

WORKDIR /app

ENTRYPOINT ["poetry"]
CMD ["run", "pytest"]

RUN pip install poetry

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install

COPY . .
