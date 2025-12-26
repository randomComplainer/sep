#!/usr/bin/env bash

set -e;

# create a new instance if it does not exist, return the instance id

readonly API_KEY=${VULTR_API_KEY:?api key is not set}

readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"))

readonly instance_id_list=$(curl "https://api.vultr.com/v2/instances?label=sep-server" \
	-s \
	-X GET \
	-H "Authorization: Bearer ${API_KEY}" \
	| jq -r '.instances[].id');

if [[ -z "${instance_id_list}" ]]; then
	echo "creating a new server" >&2
	${script_dir}/instance_create.sh
else
	echo "server already exists" >&2
	echo "${instance_id_list}"
fi
