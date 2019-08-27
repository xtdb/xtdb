#! /usr/bin/env sh

rm -r resources/static/crux-ui/compiled

node_modules/.bin/shadow-cljs release app

node_modules/.bin/shadow-cljs release app-perf

gzip -9k resources/static/crux-ui/compiled/*

ls -lah resources/static/crux-ui/compiled
