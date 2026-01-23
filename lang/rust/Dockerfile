FROM rust:1.93

WORKDIR /app

COPY Cargo.toml .
COPY src/ src/

RUN cargo fetch

ENTRYPOINT ["cargo"]
CMD ["test"]

COPY tests/ tests/
