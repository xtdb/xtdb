#! /usr/bin/env sh

function ifrm {
  [ -e "$1" ] && rm -r "$1"
}

ifrm data
ifrm checkpoint
tar xzf checkpoint.tar.gz
clj -m restore
ifrm checkpoint
echo "restored!"
