#! /usr/bin/env sh

rm checkpoint.tar.gz
clj -m backup
tar czf checkpoint.tar.gz checkpoint
