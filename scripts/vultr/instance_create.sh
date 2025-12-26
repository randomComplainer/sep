#!/usr/bin/env bash

set -e;

# create a new instance, return the instance id

readonly API_KEY=${VULTR_API_KEY:?api key is not set}

readonly REGION_ID=nrt; # Tokyo

# switch to ipv6 when sep supports it
readonly PLAN_ID=vc2-1c-1gb; # Vultr Cloud 2 - 1 CPU - 1 GB RAM
readonly OS=2625; # Debian 13 x64 (trixie)
readonly SSH_KEY_ID="85470f80-9772-4127-88ca-ce645f260379";

readonly post_data="{
	\"region\" : \"${REGION_ID}\",
	\"plan\" : \"${PLAN_ID}\",
	\"label\" : \"sep-server\",
	\"os_id\" : ${OS},
	\"backups\" : \"disabled\",
	\"sshkey_id\" : [\"${SSH_KEY_ID}\"]
}";

readonly instance_id=$(curl "https://api.vultr.com/v2/instances" \
	-s \
  -X POST \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
	--data "${post_data}" \
	| jq -r '.instance.id');

echo "${instance_id}"
