#! /bin/env bash
wget https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz
tar -xf devices_small.tar.gz -C resources
rm devices_small.tar.gz
