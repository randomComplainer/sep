#!/usr/bin/env bash
set -e

server_addr=${1:?missing server addr}

readonly script_dir=$(dirname $(realpath "$0"))
wget "ftp://ftpuser:sep-user@${server_addr}/sep-client" -O ${script_dir}/sep-client
chmod u+x ${script_dir}/sep-client

mkdir -p ${script_dir}/log

${script_dir}/sep-client --key pass --server ${server_addr}:1081 --log-format json \
	| tee "${script_dir}/log/$(date +"%Y-%m-%d_%H%M")"

