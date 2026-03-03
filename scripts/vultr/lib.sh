ssh_key_list() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	curl "https://api.vultr.com/v2/ssh-keys" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}" 
}

region_list() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	curl "https://api.vultr.com/v2/regions" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}"
}

plan_list() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	curl "https://api.vultr.com/v2/plans" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}"
}

instance_list() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	local filters=${1};

	# curl "https://api.vultr.com/v2/instances?label=sep-server" \
	curl "https://api.vultr.com/v2/instances?${filters}" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}"
}

# outputs instance_id
instance_create() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	local post_data=${1};

	local instance_id=$(curl "https://api.vultr.com/v2/instances" \
		-s \
		-X POST \
		-H "Authorization: Bearer ${API_KEY}" \
		-H "Content-Type: application/json" \
		--data "${post_data}" \
		| jq -r '.instance.id');

	echo "${instance_id}"
}

instance_delete() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	local instance_id=${1:?missing instance_id to delete};

	curl "https://api.vultr.com/v2/instances/${instance_id}" \
		-s \
		-X DELETE \
		-H "Authorization: Bearer ${API_KEY}"
}

instance_get() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	local instance_id=${1:?missing instance_id to get};

	curl "https://api.vultr.com/v2/instances/${instance_id}" \
		-s \
		-X GET \
		-H "Authorization: Bearer ${API_KEY}"
}

instance_wait_for_ip() {
	local API_KEY=${VULTR_API_KEY:?api key is not set};
	local instance_id=${1:?missing instance_id to wait};

	while true; do
		local instance_info=$(instance_get ${instance_id});
		local instance_state=$(echo "${instance_info}" | jq -r '.instance.status');

		if [[ "${instance_state}" == "active" ]]; then
			echo "${instance_info}" | jq -r '.instance.main_ip';
			exit 0;
		fi

		echo "instance is not ready yet, sleeping..." >&2;
		sleep 10;
	done
}
