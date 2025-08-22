#!/usr/bin/env bash

set -e

service_exists() {
    local service_name=$1
    if [[ $(systemctl list-units --all -t service --full --no-legend "$service_name.service" | sed 's/^\s*//g' | cut -f1 -d' ') == $service_name.service ]]; then
        return 0
    else
        return 1
    fi
}

KEY=${1:?missing key}

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
	# only available in debian 13+
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

SERVICE_NAME=sep-server
BIN_PATH=/usr/local/bin/$SERVICE_NAME

systemctl stop sep-server || true
systemctl disable sep-server || true

sed -i "s/{key}/${KEY}/g" ./service/sep-server.service 

install -m 755 target/release/$SERVICE_NAME $BIN_PATH
install -m 644 ./service/$SERVICE_NAME.service /etc/systemd/system/

systemctl daemon-reexec
systemctl daemon-reload
systemctl enable $SERVICE_NAME
systemctl restart $SERVICE_NAME

