#!/usr/bin/env bash
URL="file://$(pwd)/build/site/index.html"

[[ -x $BROWSER ]] && exec "$BROWSER" "$URL"

path=$(which xdg-open || which gnome-open) && exec "$path" "$URL"

echo "Couldn't open browser, here's the URL to open:"
echo $URL
