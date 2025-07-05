#!/usr/bin/env bash

set -e

SERVICE_NAME=sep-client
BIN_PATH=/usr/local/bin/$SERVICE_NAME
install -m 755 target/release/$SERVICE_NAME $BIN_PATH
install -m 644 ./service/$SERVICE_NAME.service /etc/systemd/system/

systemctl daemon-reexec
systemctl daemon-reload
systemctl enable $SERVICE_NAME
systemctl restart $SERVICE_NAME

systemctl status $SERVICE_NAME
