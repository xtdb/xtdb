#!/usr/bin/env bash
wget https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz
wget https://timescaledata.blob.core.windows.net/datasets/weather_small.tar.gz
mkdir resources
tar -xf devices_small.tar.gz -C resources
tar -xf weather_small.tar.gz -C resources
rm devices_small.tar.gz
rm weather_small.tar.gz
