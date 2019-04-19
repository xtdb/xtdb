#! /usr/bin/env sh

[ ! -e "data" ] && echo "no data dir found.\ntry restore.sh first\nexiting" && exit 0

function ifrm {
  [ -e "$1" ] && rm -r "$1"
}

ifrm checkpoint.tar.gz
clj -m backup
tar czf checkpoint.tar.gz checkpoint
rm -r checkpoint
