#!/usr/bin/env bash

set -e


apt-get update;

if ! dpkg -s vsftpd; then
	apt-get install -y vsftpd;
fi

if ! id -u ftpuser; then
	useradd -m -d /srv/ftpdownloads -s /usr/sbin/nologin ftpuser;
	(echo "sep-user"; echo "sep-user") | passwd ftpuser;
	chmod 555 /srv/ftpdownloads
	chown ftpuser:ftpuser /srv/ftpdownloads
fi

sed -i 's/listen=NO/listen=YES/g' /etc/vsftpd.conf
sed -i 's/listen_ipv6=YES/listen_ipv6=NO/g' /etc/vsftpd.conf
sed -i 's/#chroot_local_user=YES/chroot_local_user=YES/g' /etc/vsftpd.conf

systemctl restart vsftpd
