#!/usr/bin/env bash

set -e;

readonly API_KEY=${VULTR_API_KEY:?api key is not set};
readonly SEP_KEY=${1:?missing sep key};

readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
readonly project_dir=$(realpath "${script_dir}/../..");
cd "${project_dir}";

# ${script_dir}/instance_delete.sh;
readonly instance_id=$(${script_dir}/ensure_server_exists.sh);

cargo build --release;

readonly ipv4=$(${script_dir}/wait_for_instance_to_be_ready.sh "${instance_id}");
echo "server at ${ipv4}" >&2;

while true; do
	if nc -z -w1 "${ipv4}" 22; then
		break;
	fi

	echo "waiting for server's ssh to be ready" >&2;
	sleep 5;
done

scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	"${project_dir}/target/release/sep-server" \
	root@${ipv4}:/usr/local/bin/sep-server;

scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	"${project_dir}/service/sep-server.service" \
	root@${ipv4}:/etc/systemd/system/sep-server.service;

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${ipv4} << EOF
  ufw allow 1081;
	sed -i 's/{key}/${SEP_KEY}/g' /etc/systemd/system/sep-server.service;

	systemctl stop sep-server || true;
	systemctl disable sep-server || true;

	systemctl daemon-reexec;
	systemctl daemon-reload;
	systemctl enable sep-server;
	systemctl restart sep-server;
EOF

echo "server deployed at ${ipv4}" >&2;

