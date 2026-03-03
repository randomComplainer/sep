#!/usr/bin/env bash

main() {
	set -e;

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

		local server_ip=$( \
			echo ${instances} \
			| jq -r '.main_ip' \
			| fzf --header "select server to delete");

		echo ${instances} | jq -r "select(.main_ip==\"${server_ip}\") | .id";
	);

	instance_delete ${instance_id};
}

main $@

