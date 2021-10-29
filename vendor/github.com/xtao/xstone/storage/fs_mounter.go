package storage

import (
	"errors"
	. "github.com/xtao/xstone/common"
	. "github.com/xtao/xstone/config"
)

type FSMounter interface {
	MountVolume(vol string, mountpoint string, timeout int) (error, string)
    GetManifest() *StorageManifest
}

func NewFSMounter(config *StorageClusterMountConfig) FSMounter {
	switch config.FSType {
	case FS_NFS:
		return &nfsMounter{
            commonMounter: commonMounter{
			        config: *config,
            },
		}
	case FS_GLUSTER, FS_ALAMO:
		return &glusterfsMounter{
            commonMounter: commonMounter{
			        config: *config,
            },
		}
	case FS_CEPH, FS_ANNA:
		return &cephfsMounter{
            commonMounter: commonMounter{
			        config: *config,
            },
		}
    case FS_HDFS:
        return &hdfsMounter{
            commonMounter: commonMounter{
			        config: *config,
            },
        }
	case OSS_TENCENT:
		return &tencentOsMounter{
			commonMounter: commonMounter{
				config: *config,
			},
		}
	case OSS_ALIBABA:
		return &alibabaOsMounter{
			commonMounter: commonMounter{
				config: *config,
			},
		}
    case OSS_RGW:
		return &rgwOsMounter{
			commonMounter: commonMounter{
				config: *config,
			},
		}
	default:
		StoneLogger.Infof("FS Type %s not supported\n",
			config.FSType)
	}

	return nil
}

type commonMounter struct {
	config StorageClusterMountConfig
}

func (mounter *commonMounter) GetManifest() *StorageManifest {
    return &StorageManifest{
            Cluster: mounter.config.ClusterName,
            Volume: mounter.config.Volume,
            FSType: mounter.config.FSType,
    }
}

func (mounter *commonMounter) Config() *StorageClusterMountConfig {
    return &mounter.config
}

type nfsMounter struct {
    commonMounter
}


func (mounter *nfsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
	if mountpoint == "" {
		StoneLogger.Infof("Can't mount empty mountpoint\n")
		return errors.New("Empty mount point"), ""
	}

    config := mounter.Config()
	command := "mount -t nfs "
	if len(config.Servers) <= 0 {
		StoneLogger.Infof("No mount server exist for cluster %s NFS, fail mount volume %s\n",
			config.ClusterName, vol)
		return errors.New("No mount server for cluster " + config.ClusterName),
            ""
	}
	/*use the first server by default*/
	mntSrc := config.Servers[0]
	if config.MountPath == "" {
		mntSrc += ":/" + vol
	} else {
		mntSrc += ":/" + config.MountPath
	}
	mntTarget := mountpoint
	options := config.Options
	command += mntSrc + " " + options + " " + mntTarget

	StoneLogger.Infof("Will mount volume %s of cluster %s type %s to %s by %s\n",
		vol, config.ClusterName, config.FSType, mountpoint,
		command)

	err := RunCommandAsync(command, timeout)
	if err != nil {
		StoneLogger.Infof("Fail to run mount comand %s: %s\n",
			command, err.Error())
		return err, ""
	}

	StoneLogger.Infof("The vol %s type %s of cluster %s mounted on %s\n",
		vol, config.FSType, mounter.config.ClusterName, mountpoint)

	return nil, mountpoint
}

type glusterfsMounter struct {
    commonMounter
}

func (mounter *glusterfsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
	if mountpoint == "" {
		StoneLogger.Infof("Can't mount empty mountpoint\n")
		return errors.New("Empty mount point"), ""
	}

    config := mounter.Config()
	command := "mount -t glusterfs "
	if len(config.Servers) <= 0 {
		StoneLogger.Infof("No mount server exist for cluster %s NFS, fail mount volume %s\n",
			config.ClusterName, vol)
		return errors.New("No mount server for cluster " + config.ClusterName),
            ""
	}
	/*use the first server by default*/
	mntSrc := config.Servers[0] + ":" + vol
	mntTarget := mountpoint
	options := ""
	command += mntSrc + " " + options + " " + mntTarget

	StoneLogger.Infof("Will mount volume %s of cluster %s type %s to %s by %s\n",
		vol, config.ClusterName, mounter.config.FSType, mountpoint,
		command)

	err := RunCommandAsync(command, timeout)
	if err != nil {
		StoneLogger.Infof("Fail to run mount comand %s: %s\n",
			command, err.Error())
		return err, ""
	}

	StoneLogger.Infof("The vol %s type %s of cluster %s mounted on %s\n",
		vol, config.FSType, mounter.config.ClusterName, mountpoint)

	return nil, mountpoint
}

type cephfsMounter struct {
    commonMounter
}

func (mounter *cephfsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
    if mountpoint == "" {
        StoneLogger.Infof("Can't mount empty mountpoint\n")
        return errors.New("Empty mount point"), ""
    }

    config := mounter.Config()
    command := "ceph-fuse "
    /*Now the ceph vol support mount with no parameter -m <server>*/
    /****************************************************
    *if len(config.Servers) <= 0 {
    *	StoneLogger.Infof("No mount server exist for cluster %s, fail mount volume %s\n",
    *		config.ClusterName, vol)
    *	return errors.New("No mount server for cluster " + config.ClusterName),
    *        ""
    *}
    *****************************************************/
    var mntSrc string
    lenServers := len(config.Servers)
    if lenServers == 0 {
        mntSrc = ""
    } else {
        mntSrc = "-m "
        for k, v := range config.Servers {
            if k == (lenServers-1) {
	            mntSrc += v
            } else {
                mntSrc += v + ","
            }
        }
    }
    mntTarget := mountpoint
    options := "--cluster " + config.ClusterName + " " + mounter.config.Options
    command += mntSrc + " " + mntTarget + " " + options

    StoneLogger.Infof("Will mount volume %s of cluster %s type %s to %s by %s\n",
	    vol, config.ClusterName, mounter.config.FSType, mountpoint,
	    command)

    err := RunCommandAsync(command, timeout)
    if err != nil {
        StoneLogger.Infof("Fail to run mount comand %s: %s\n",
            command, err.Error())
        return err, ""
    }

    StoneLogger.Infof("The vol %s type %s of cluster %s mounted on %s\n",
        vol, config.FSType, mounter.config.ClusterName, mountpoint)

    return nil, mountpoint
}


/*hdfs mounter do nothing except return its normalized path*/
type hdfsMounter struct {
    commonMounter
}

/* HDFS is accessed via client, so mount volume action just build a
 * normalized uri to caller.
 */
func (mounter *hdfsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
    config := mounter.Config()
	mntSrc := config.Servers[0]
    normalizedPath := "hdfs://" + mntSrc
	StoneLogger.Infof("The vol %s type %s of cluster %s mounted on %s\n",
		vol, config.FSType, mounter.config.ClusterName,
        normalizedPath)

	return nil, normalizedPath
}

/*tencent os mounter do nothing except return its normalized path*/
type tencentOsMounter struct {
	commonMounter
}

func (mounter *tencentOsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
	return nil, ""
}

/*tencent os mounter do nothing except return its normalized path*/
type alibabaOsMounter struct {
	commonMounter
}

func (mounter *alibabaOsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
	return nil, ""
}

/*rgw os mounter do nothing except return its normalized path*/
type rgwOsMounter struct {
	commonMounter
}

func (mounter *rgwOsMounter) MountVolume(vol string, mountpoint string,
	timeout int) (error, string) {
	return nil, ""
}
