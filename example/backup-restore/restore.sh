#! /usr/bin/env sh

tar xzf checkpoint.tar.gz
rm -r data
clj -m restore
