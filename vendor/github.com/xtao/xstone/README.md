#The xstone readme

## clone the source code from git
```
git@192.168.12.50:/home/git/repositories/xstone.git
```

## Make rpm package
The module you must know the go path and go root what is used for, you clone the
source code to go path.
```
make rpm
```

## Install
Last step make the rpm package in $GOPATH/xstone/rpm/rpmbuild/RPMS/x86_64/xstone-server-1.0-0.x86_64.rpm
```
rpm -ivh xstone-server-1.0-0.x86_64.rpm
```

## Start service
When the rpm install in the node you can start the service according systemctl command.
```
systemctl enable automount-manager-plugin
systemctl start automount-manager-plugin
systemctl status automount-manager-plugin
```
You can know the service status is running,which can support automount function.
