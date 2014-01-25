#!/bin/sh

# This script is used to provision a Vagrant VM for compiling this project on a Mac
# for deployment to Linux.

sudo apt-get update
sudo apt-get -y install make
sudo apt-get -y install git-core
tarball="go1.2.linux-amd64.tar.gz"
wget https://go.googlecode.com/files/$tarball
tar -zxf $tarball
echo 'export GOROOT=$HOME/go' >> .profile
echo 'export PATH=$GOROOT/bin:$PATH' >> .profile
echo 'export REPOS=~' >> .profile
