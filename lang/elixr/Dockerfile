FROM elixir:1.17.3
RUN apt-get clean && apt-get update && apt-get install -y git postgresql-client

WORKDIR /app

ENTRYPOINT ["mix"]
CMD ["test"]

COPY mix.exs mix.lock ./
RUN mix deps.get
COPY . .
