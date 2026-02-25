#!/usr/bin/env bash

(
	set -e;

	readonly API_KEY=${VULTR_API_KEY:?api key is not set};

	readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
	readonly project_dir=$(realpath "${script_dir}/../..");
	cd "${project_dir}";

	setup_http() {
		local server_ip=${1:?missing server ip};

		ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  root@${server_ip} << EOF
	set -e;

	mkdir -p /root/speed_test;
	if [[ ! -f /root/speed_test/test_file ]]; then
		dd if=/dev/urandom bs=1M count=32 of=/root/speed_test/test_file;
	fi

	kill \$(pidof busybox) || true;
	busybox httpd -h /root/speed_test/

	ufw allow 80;
EOF
	}

	main() {
		local cmd=${1:?missing command};

		local server_ip=$(curl "https://api.vultr.com/v2/instances?label=sep-server" \
			-s \
			-X GET \
			-H "Authorization: Bearer ${API_KEY}" \
		| jq -r '.instances[].main_ip');

		case "${cmd}" in
			raw)
				ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  \
					root@${server_ip} 'dd if=/dev/zero bs=16M' | pv > /dev/null
				;;
			http)
				setup_http "${server_ip}";
				curl "http://${server_ip}:80/test_file" > /dev/null;
				;;
			proxy)
				setup_http "${server_ip}";
				all_proxy=socks5://127.0.0.1:1082 curl "http://${server_ip}:80/test_file" > /dev/null;
				;;
			*)
				echo "unknown command: ${cmd}";
				;;
		esac
	}

	main $@;
)
