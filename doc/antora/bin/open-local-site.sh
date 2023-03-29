#!/usr/bin/env bash
URL="file://$(pwd)/build/site/main/index.html"

[[ -x $BROWSER ]] && exec "$BROWSER" "$URL"

path=$(which xdg-open || which gnome-open || which open) && exec "$path" "$URL"

echo "Couldn't open browser, here's the URL to open:"
echo $URL
