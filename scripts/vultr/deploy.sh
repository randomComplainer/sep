#!/usr/bin/env bash

main(){
	set -e;

	local SEP_KEY=${1:?missing sep key};

	local script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
	local project_dir=$(realpath "${script_dir}/../..");
	cd "${project_dir}";
	. $script_dir/lib.sh;

	local label=sep-server

	local instance_id=$( \
		local instances=$(instance_list label=${label} \
			| jq '.instances[]' \
			| jq '{ id: .id, main_ip: .main_ip}' \
		);

		if [[ ! -z ${instances} ]]; then
			local server_ip=$( \
				echo ${instances} \
				| jq -r '.main_ip' \
				| { cat; echo new server; } \
				| fzf --header "select server to deploy");

			if [[ ! $server_ip == "new server" ]]; then
				echo ${instances} | jq -r "select(.main_ip==\"${server_ip}\") | .id";
				exit 0;
			fi
		fi

		# create new server
		local REGION_ID=nrt; # Tokyo
		local PLAN_ID=vc2-1c-1gb; # Vultr Cloud 2 - 1 CPU - 1 GB RAM
		local OS=2625; # Debian 13 x64 (trixie)
		local SSH_KEY_ID="85470f80-9772-4127-88ca-ce645f260379";
		local post_data="{
				\"region\" : \"${REGION_ID}\",
				\"plan\" : \"${PLAN_ID}\",
				\"label\" : \"sep-server\",
				\"os_id\" : ${OS},
				\"backups\" : \"disabled\",
				\"enable_ipv6\": true,
				\"sshkey_id\" : [\"${SSH_KEY_ID}\"]
			}";

		instance_create "${post_data}"
	);

	echo instance_id ${instance_id} >&2;

	cargo build --release;

	local ip=$(instance_wait_for_ip ${instance_id});
	echo "server at ${ip}" >&2;

	while true; do
		# default nc is an old version that does not support ipv6
		# apt install ncat
		if nc -z -w1 "${ip}" 22; then
			break;
		fi

		echo "waiting for server's ssh to be ready" >&2;
		sleep 5;
	done

	ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${ip} >&2 <<EOF
		ufw allow 1081;
		systemctl stop sep-server || true;
		systemctl disable sep-server || true;
EOF

	scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
		"${project_dir}/target/release/sep-server" \
		root@${ip}:/usr/local/bin/sep-server >&2;

	scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
		"${project_dir}/service/sep-server.service" \
		root@[${ip}]:/etc/systemd/system/sep-server.service >&2;

	bound_ip=${ip};

	ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${ip} >&2 <<EOF
		sed -i 's/{key}/${SEP_KEY}/g' /etc/systemd/system/sep-server.service;
		sed -i 's/{bound_addr}/${bound_ip}:1081/g' /etc/systemd/system/sep-server.service;

		systemctl daemon-reexec;
		systemctl daemon-reload;
		systemctl enable sep-server;
		systemctl restart sep-server;
EOF

	echo "server deployed at ${ip}" >&2;
	echo ${ip}
}

main $@
