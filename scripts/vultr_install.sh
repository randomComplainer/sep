#!/usr/bin/env bash

set -e

apt-get update;

if ! dpkg -s vsftpd; then
	apt-get install -y vsftpd;

	sed -i 's/listen=NO/listen=YES/g' /etc/vsftpd.conf
	sed -i 's/listen_ipv6=YES/listen_ipv6=NO/g' /etc/vsftpd.conf
	sed -i 's/#chroot_local_user=YES/chroot_local_user=YES/g' /etc/vsftpd.conf
	echo "check_shell=NO" >> /etc/vsftpd.conf
	echo "/usr/sbin/nologin" >> /etc/shells
fi

if ! id -u ftpuser; then
	useradd -m -d /srv/ftpdownloads -s /usr/sbin/nologin ftpuser;
	(echo "sep-user"; echo "sep-user") | passwd ftpuser;
	chmod 555 /srv/ftpdownloads;
	chown ftpuser:ftpuser /srv/ftpdownloads;
fi

systemctl restart vsftpd

if ! -x "$(which rustup)"; then
	apt-get install -y rustup;
	rustup update nightly;
	apt install mingw-w64;
	rustup target add x86_64-pc-windows-gnu;
	rustup default nightly;
fi

if ! -e /swapfile; then
	fallocate -l 2G /swapfile
	chmod 600 /swapfile
	mkswap /swapfile
	swapon /swapfile
fi

cargo build --release;
cargo build --release --target x86_64-pc-windows-gnu;

cp ./target/release/sep-client /srv/ftpdownloads/;
cp ./target/x86_64-pc-windows-gnu/release/sep-client.exe /srv/ftpdownloads/;

# wget "ftp://ftpuser:sep-user@{}/sep-client" 

