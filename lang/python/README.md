# XTDB Python SDK

This is the Python SDK for the [XTDB](https://xtdb.com) database.


## Development

This library uses [Poetry](https://python-poetry.org/) for dependency management. 
To install the dependencies, run:

```shell
poetry install
```


## Testing

See the test harness README for how to nodes are spun up and down.

You can run the http-proxy locally via the test harness:

`./gradlew :lang:test-harness:httpProxy`

This will run the http-proxy server on `http://localhost:3300`.
You can then run the tests via:

```shell
poetry run pytest
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
