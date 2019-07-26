#! /usr/bin/env sh

rm -r resources/static/crux-ui/compiled

shadow-cljs release :app

shadow-cljs release :app-perf

gzip -9k resources/static/crux-ui/compiled/*

ls -lah resources/static/crux-ui/compiled
