#!/usr/bin/env bash
mkdir data
curl -L https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz | tar xz -C data
curl -L https://timescaledata.blob.core.windows.net/datasets/weather_small.tar.gz | tar xz -C data
