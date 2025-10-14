#! /bin/bash

# for vps/vms with low memory
fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
