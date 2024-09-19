# XTDB Python client library

This will be the home of the XTDB client library for Python.

In the main, users should use standard PostgreSQL tooling to connect to XTDB - this repo will contain helper functions for advanced usage.

## Development

This library uses [Poetry](https://python-poetry.org/) for dependency management. 
To install the dependencies, run:

```shell
poetry install
```

## Testing

See the test harness README for how to nodes are spun up and down.

```shell
poetry run pytest
```

There is also the option of running a static typechecker
```shell
poetry run mypy
```

## Deployment

You'll need an account on [PyPI](https://pypi.org) to deploy the library.

Create yourself an API token on PyPI and save it using Poetry:

```shell
poetry config pypi-token.pypi <your-api-token>
```

To deploy a new version of the library, update the version number in `pyproject.toml` and run:

```shell
poetry publish --build
```
