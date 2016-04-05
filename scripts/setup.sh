#!/bin/sh

#install paraprox
sudo cp ../paraproxio.py /usr/bin/paraproxio.py

#install systemd
sudo cp ../contrib/paraprox.service /lib/systemd/system/paraprox.service
sudo chmod 644 /lib/systemd/system/paraprox.service
sudo systemctl daemon-reload
sudo systemctl enable paraprox.service

#create log dir
sudo mkdir -p /var/log/paraprox

#create logfile
sudo touch /var/log/paraprox/access.log

