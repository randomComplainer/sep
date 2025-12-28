#!/usr/bin/env bash
set -e

readonly API_KEY=${VULTR_API_KEY:?api key is not set};
readonly SEP_KEY=${1:?missing sep key};

readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
readonly project_dir=$(realpath "${script_dir}/../..");

cd "${project_dir}";

readonly server_ip=$(curl "https://api.vultr.com/v2/instances?label=sep-server" \
	-s \
	-X GET \
	-H "Authorization: Bearer ${API_KEY}" \
	| jq -r '.instances[].main_ip');
	# | jq -r '.instances[].v6_main_ip');

cargo run --release --bin sep-client -- \
	--bound-addr 0.0.0.0:1082 \
	--key ${SEP_KEY} \
	--server ${server_ip}:1081 \
	--log-format json \
	| tee ./log/vultr.client.json

