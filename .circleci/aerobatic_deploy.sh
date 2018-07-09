#!/bin/sh
curl -sL https://deb.nodesource.com/setup_10.x | sudo -E bash -
sudo apt-get install npm
sudo npm install aerobatic-cli -g
sudo npm install sass -g
cd ~/crux/website
sass assets/scss/style.scss assets/css/style.css
aero deploy
