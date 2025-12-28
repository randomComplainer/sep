#!/usr/bin/env bash

set -e;

export use_ipv6=false;

readonly API_KEY=${VULTR_API_KEY:?api key is not set};
readonly SEP_KEY=${1:?missing sep key};

readonly script_dir=$(dirname $(realpath "${BASH_SOURCE[0]}"));
readonly project_dir=$(realpath "${script_dir}/../..");
cd "${project_dir}";

# ${script_dir}/instance_delete.sh;
readonly instance_id=$(${script_dir}/ensure_server_exists.sh);

cargo build --release;

ip=$(${script_dir}/wait_for_instance_to_be_ready.sh "${instance_id}");
echo "server at ${ip}" >&2;

if [[ "${use_ipv6}" == "true" ]]; then
	ip="[${ip}]";
fi

while true; do
	# default nc is an old version that does not support ipv6
	# apt install ncat
	if nc -z -w1 "${ip}" 22; then
		break;
	fi

	echo "waiting for server's ssh to be ready" >&2;
	sleep 5;
done

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${ip} << EOF
  ufw allow 1081;
	systemctl stop sep-server || true;
	systemctl disable sep-server || true;
EOF

scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	"${project_dir}/target/release/sep-server" \
	root@${ip}:/usr/local/bin/sep-server;

scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	"${project_dir}/service/sep-server.service" \
	root@[${ip}]:/etc/systemd/system/sep-server.service;

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${ip} << EOF
	sed -i 's/{key}/${SEP_KEY}/g' /etc/systemd/system/sep-server.service;
	sed -i 's/{bound_addr}/${ip}:1081/g' /etc/systemd/system/sep-server.service;

	systemctl daemon-reexec;
	systemctl daemon-reload;
	systemctl enable sep-server;
	systemctl restart sep-server;
EOF

echo "server deployed at ${ip}" >&2;

