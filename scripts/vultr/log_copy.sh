#!/usr/bin/env bash

set -e

readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
readonly project_dir=$(realpath "${script_dir}/../..");
readonly log_dir="${project_dir}/log";

readonly server_ip=$(${script_dir}/instance_list.sh | jq -r '.instances[].main_ip');

scp \
	-o StrictHostKeyChecking=no \
	-o UserKnownHostsFile=/dev/null \
	root@${server_ip}:/tmp/sep-server.json \
	${log_dir}/vultr.server.json
