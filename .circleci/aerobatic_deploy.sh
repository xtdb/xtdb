#!/bin/sh
curl -sL https://deb.nodesource.com/setup_10.x | sudo -E bash -
sudo apt-get install npm
sudo npm install aerobatic-cli -g
sudo npm instal sass
cd ~/crux/website
sass assets/scss/style.css assets/css/style.css
aero deploy
