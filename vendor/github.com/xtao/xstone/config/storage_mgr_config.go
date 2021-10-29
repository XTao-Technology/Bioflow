package config

import (
)

/*
 * config for mount a file system volume, support:
 * 1) NFS
 * 2) glusterfs
 * 3) ceph
 * 4) hdfs
 */
const (
    FS_GLUSTER string = "gluster"
    FS_CEPH string = "ceph"
    FS_NFS string = "nfs"
    FS_ALAMO string = "alamo"
    FS_ANNA string = "anna"
    FS_HDFS string = "hdfs"
    OSS_TENCENT string = "oss-tencent"
    OSS_ALIBABA string = "oss-alibaba"
    OSS_RGW string = "oss-rgw"
)

type StorageClusterMountConfig struct {
    FSType string
    ClusterName string
    Volume string
    Servers []string
    Options string
    MountPath string
}

type SchedulerStoreConfig struct {
    SchedVolMntPath map[string]string
    AutoMount bool
}

type ServerStoreConfig struct {
    VolMntPath map[string]string
    TempMnt string
	JobRootPath string
}

type ContainerStoreConfig struct {
    ContainerDataDir string
    ContainerWorkDir string
    ContainerTempDir string
}

type StoreMgrConfig struct {
    SchedulerConfig SchedulerStoreConfig
    ServerConfig ServerStoreConfig
    ContainerConfig ContainerStoreConfig
}

type StorageManifest struct {
    Cluster string
    Volume string
    FSType string
}
