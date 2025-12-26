#!/usr/bin/env bash
set -e

readonly API_KEY=${VULTR_API_KEY:?api key is not set};

curl "https://api.vultr.com/v2/ssh-keys" \
	-s \
	-X GET \
	-H "Authorization: Bearer ${API_KEY}" \
	| jq
