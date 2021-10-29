# automount-manager-plugin for xstone

##plugin
The package plugin only for plugin function which indludes all volume plugin API.

##server
The package server only for http server which used for auto mount function.We
can use it with next command.

```
curl  -X POST -H "Content-Type:application/json"
"http://<server-ip>:8188/v1/auto/mount?Volume=<clusterVolume>&ClusterName=<clusterName>&MountPoint=<mountPoint>&PluginVolume=<pluginVolume>"
```
###records
1.The Volume and ClusterName need we pre-configured with bioadm command.
2.The PluginVolume also need we pre-create.
3.The MountPoint Users can optionally choose.
