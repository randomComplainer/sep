#!/usr/bin/env bash

set -e;

# wait for given instance specified by the instance id to be ready
# return the ip

readonly API_KEY=${VULTR_API_KEY:?api key is not set};
readonly instance_id=${1:?missing instance id};

while true; do
	instance_info=$(curl "https://api.vultr.com/v2/instances/${instance_id}" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}");

	instance_state=$(echo "${instance_info}" | jq -r '.instance.status');
	echo "instance state: ${instance_state}" >&2;

	if [[ "${instance_state}" == "active" ]]; then
		if [[ "${use_ipv6}" == "true" ]]; then
			echo "${instance_info}" | jq -r '.instance.v6_main_ip';
		else
			echo "${instance_info}" | jq -r '.instance.main_ip';
		fi
		break;
	fi
	
	echo "instance is not ready yet, sleeping..." >&2;
	sleep 10;
done

