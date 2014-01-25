Vagrant.configure("2") do |config|
  config.vm.box = "precise64"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"
  config.vm.host_name = "gumshoedb-vagrant"
  config.vm.network "forwarded_port", guest: 9000, host: 9001
  config.vm.synced_folder ".", "/home/vagrant/gumshoedb"
  config.vm.provision "shell", path: "script/provision_vagrant.sh"
end
