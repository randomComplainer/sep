#!/usr/bin/env bash

main() {
	set -e

	local script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
	local project_dir=$(realpath "${script_dir}/../..");
	. $script_dir/lib.sh;

	server_ip=$(instance_select_ip "select server to connect");

	exec ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${server_ip} $@ >&2
}

main $@

