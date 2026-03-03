#!/usr/bin/env bash

main() {
	set -e

	readonly SEP_KEY=${1:?missing sep key};

	readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
	readonly project_dir=$(realpath "${script_dir}/../..");
	cd "${project_dir}";
	. $script_dir/lib.sh;

	server_ip=${2};

	echo server_ip: $server_ip >&2

	if [[ -z ${server_ip} ]]; then
		server_ip=$(
				instance_list label=sep-server \
				| jq '.instances[]' \
				| jq -r '.main_ip' \
				| fzf --header "select server" \
			);
	fi

	echo "server ip: ${server_ip}" >&2;

	cargo run --release --bin sep-client -- \
		--bound-addr 0.0.0.0:1082 \
		--key ${SEP_KEY} \
		--server ${server_ip}:1081 \
		--log-format json \
		| tee ./log/vultr.client.json \
		| rg "(DEBUG)|(WARN)|(ERROR)";
 }

main $@
