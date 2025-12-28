#!/usr/bin/env bash

set -e;

# delete all instances with label "sep-server"

readonly API_KEY=${VULTR_API_KEY:?api key is not set}

curl "https://api.vultr.com/v2/plans" \
	-s \
	-X GET \
	-H "Authorization: Bearer ${API_KEY}" \
	| jq

        # "ewr",
        # "atl"
