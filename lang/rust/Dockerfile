FROM python:3.15 AS adbc-drivers

RUN pip install dbc==0.3.1 \
    && dbc install --level system "flightsql=1.13.0"

FROM rust:1.93

COPY --from=adbc-drivers /etc/adbc /etc/adbc

WORKDIR /app

COPY Cargo.toml .
COPY src/ src/

RUN cargo fetch

ENTRYPOINT ["cargo"]
CMD ["test", "--", "--test-threads=1"]

COPY tests/ tests/
