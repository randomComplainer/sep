#!/usr/bin/env bash

main() {
	set -e

	local script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
	local project_dir=$(realpath "${script_dir}/../..");
	local log_base_dir="${project_dir}/log";
	local log_dir="${log_base_dir}/vultr/";
	cd "${project_dir}";
	. $script_dir/lib.sh;
	mkdir -p ${log_dir};

	local server_ip=${1};

	if [[ -z ${server_ip} ]]; then
		local instances=$(instance_list label=sep-server \
			| jq '.instances[]' \
			| jq '{ id: .id, main_ip: .main_ip}' \
		);

		if [[ -z ${instances} ]]; then
			echo 'no active server' >&2;
			exit 1
		fi

		server_ip=$(echo ${instances} \
		| jq -r '.main_ip' \
		| fzf --header "select server to copy logs");
	fi

	cp ${log_base_dir}/vultr.client.json ${log_dir}/client.json;

	ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${server_ip} >&2 <<EOF
		journalctl -u sep-server -o cat --since '5 min ago' > /root/vultr.server.json
EOF

	scp \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		root@${server_ip}:/root/vultr.server.json \
		${log_dir}/server.json
}

main $@
