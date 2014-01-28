#!/bin/sh

# This script is used to provision a Vagrant VM for compiling this project on a Mac
# for deployment to Linux.

sudo apt-get update
sudo apt-get -y install make
sudo apt-get -y install git
tarball="go1.2.linux-amd64.tar.gz"
wget https://go.googlecode.com/files/$tarball
tar -C /usr/local -xzf $tarball
echo 'export PATH=/usr/local/go/bin:$PATH' >> .profile
echo 'export REPOS=~' >> .profile
