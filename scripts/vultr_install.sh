#!/usr/bin/env bash

set -e

apt-get update;
apt-get install -y \
	vsftpd;


useradd -m -d /srv/ftpdownloads -s /usr/sbin/nologin ftpuser;
(echo "opensesame"; echo "opensesame") passwd ftpuser;
chmod 555 /srv/ftpdownloads
chown ftpuser:ftpuser /srv/ftpdownloads

sed -i 's/listen=NO/listen=YES/g' /etc/vsftpd.conf
sed -i 's/listen_ipv6=YES/listen_ipv6=NO/g' /etc/vsftpd.conf
sed -i 's/#chroot_local_user=YES/chroot_local_user=YES/g' /etc/vsftpd.conf
