#!/usr/bin/env bash

set -e

apt-get update;
apt-get install -y \
	vsftpd;


useradd -m -d /srv/ftpdownloads -s /usr/sbin/nologin ftpuser;
(echo "opensesame"; echo "opensesame") sudo passwd ftpuser;
chmod 555 /srv/ftpdownloads
chown ftpuser:ftpuser /srv/ftpdownloads

