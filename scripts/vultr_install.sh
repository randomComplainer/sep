#!/usr/bin/env bash

set -e

sudo apt-get update;
sudo apt-get install -y \
	vsftpd;


sudo useradd -m -d /srv/ftpdownloads -s /usr/sbin/nologin ftpuser;
(echo "opensesame"; echo "opensesame") sudo passwd ftpuser;
sudo chmod 555 /srv/ftpdownloads
sudo chown ftpuser:ftpuser /srv/ftpdownloads

