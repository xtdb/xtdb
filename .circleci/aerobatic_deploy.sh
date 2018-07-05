#!/bin/sh
curl -sL https://deb.nodesource.com/setup_10.x | sudo -E bash -
sudo apt-get install -ycd  npm
sudo npm install aerobatic-cli -g
cd ~/crux/website
aero deploy
