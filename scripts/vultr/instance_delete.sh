#!/usr/bin/env bash

set -e;

# delete all instances with label "sep-server"

readonly API_KEY=${VULTR_API_KEY:?api key is not set}

readonly instance_id_list=$(curl "https://api.vultr.com/v2/instances?label=sep-server" \
	-s \
	-X GET \
	-H "Authorization: Bearer ${API_KEY}" \
	| jq -r '.instances[].id');

for instance_id in ${instance_id_list}; do
	echo "deleting ${instance_id}"
	curl "https://api.vultr.com/v2/instances/${instance_id}" \
		-s \
		-X DELETE \
		-H "Authorization: Bearer ${API_KEY}"
done

