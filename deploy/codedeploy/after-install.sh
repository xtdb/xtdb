#!/usr/bin/env bash
set -e
systemctl daemon-reload

# Fix permissions on systemd unit
sudo chmod 0755 /usr/lib/systemd/system/crux.service
sudo chown root:root /usr/lib/systemd/system/crux.service

# Fix permissions on environment file
sudo chmod -R 0755 /var/lib/crux
sudo chown -R ec2-user:ec2-user /var/lib/crux

# Fix permissions on JAR
sudo chmod 0755 /srv/crux.jar
sudo chown ec2-user:ec2-user /srv/crux.jar
