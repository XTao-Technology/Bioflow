#!/bin/sh

#--------------------------------------------------------------------
#This is a comment
#author :  liuyawei
#Email  :  yawei.liu@xtaotech.com
#slogan :  we offer more than just services is convenient.
#--------------------------------------------------------------------
######The script work for automount-manager-plugin daemon##########
#
#
#
#
#
#
#
#
#####

curl=""
pluginVolume=""

buildCurl() {
    if [ -z $AUTOMOUNTSERVER_IP_ADDR ] ; then
        echo 'no auto mount server ip addr in env, please set it first and you can set it with $export AUTOMOUNTSERVER_IP_ADDR="<automountserveripaddr>"'
        return 1
    else
        url=$AUTOMOUNTSERVER_IP_ADDR
    fi

    if [ -z $AUTOMOUNTSERVER_PORT ] ; then
        echo 'no auto mount server port in env, we use the default port:8188, make sure the automount server run in this port.'
        port="8188"
    else
        prot=$AUTOMOUNTSERVER_PORT
    fi

    curl="http://"${url}":"${port}
    return 0
}

automountFunc() {

    echo "$# $1 $2 $3"
    clustername=$1
    clustervolname=$2
    mountpoint=$3
    buildCurl
    if [ $? -eq 1 ] ; then
        echo "Build url filed"
        return 1
    else
        automounturl=${curl}"/v1/auto/mount?Volume="${clustervolname}"&ClusterName="${clustername}"&MountPoint="${mountpoint}"&PluginVolume="${pluginVolume}
        curl -X POST -H "Content-Type:application/json" $automounturl
        if [ $? -eq 0 ] ; then
            return 0;
        else
            echo "auto mount url is $automounturl"
            return 1;
        fi
    fi
}

autoumountFunc() {
    
    clustername=$1
    clustervolname=$2
    mountpoint=$3
    buildCurl
    if [ $? -eq 1 ] ; then
        echo "Build url filed"
        return 1
    else
        automounturl=${curl}"/v1/auto/umount?Volume="${clustervolname}"&ClusterName="${clustername}"&MountPoint="${mountpoint}"&PluginVolume="${pluginVolume}
        curl -X POST -H "Content-Type:application/json" $automounturl
        if [ $? -eq 0 ] ; then
            return 0;
        else
            echo "auto mount url is $automounturl"
            return 1;
        fi
    fi

}

loadPluginVolume() {
    if [ -z $PLUGINVOLUME ] ; then
        echo 'no pluginvolume in env, you can set it with $export PLUGINVOLUME="<pluginvolume>" for temporary'
        return 1
    else
        pluginVolume=$PLUGINVOLUME
    fi
    return 0
}


if [[ $# -ne 4 && $# -ne 3 ]] ; then
    echo "The script need three or two parameters"
    echo "e.g: $0 <clustername> {mount/umount} <volname> <mountpoint> or $0 {mount/umount} <clustername> <volname>"
    exit 1;
else
    
    op=$1

    loadPluginVolume
    if [ $? -ne 0 ]; then
        echo 'load plugin volume env failed'
        exit 1
    fi

    if [ "$op" == "mount" ]; then
        automountFunc $2 $3 $4
        if [ $? -eq 0 ]; then
            echo 'auto mount success.'
            exit 0
        else
            echo 'auto mount failed.'
            exit 1
        fi
    elif [ "$op" == "umount" ]; then
        autoumountFunc $2 $3 $4
        if [ $? -eq 0 ]; then
            echo 'auto umount success.'
            exit 0
        else
            echo 'auto umount failed.'
            exit 1
        fi
    else
        echo 'we only suppory the choic {mount} or {umount}'
    fi
fi
