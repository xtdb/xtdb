#!/bin/sh
apt-get install npm
npm install aerobatic-cli -g
cd ../website
aerobatic deploy
