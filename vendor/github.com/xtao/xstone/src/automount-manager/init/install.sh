#!/usr/bin/env bash

# install storage manager plugin
mkdir -p /var/run/mnt/xtao
cp storage-manager /etc/xstone/storage-manager-plugin
chmod +x /usr/bin/storage-manager-plugin


cp init/storage-manager-plugin.service /etc/systemd/system/storage-manager-plugin.service
chmod 750 /etc/systemd/system/storage-manager-plugin.service

systemctl daemon-reload
systemctl enable storage-manager-plugin
systemctl start storage-manager-plugin
systemctl status storage-manager-plugin
