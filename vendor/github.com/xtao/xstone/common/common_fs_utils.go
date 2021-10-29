package common

func FSCommonUtilsBuildVolURI(cluster string, vol string) string {
	if cluster == "" {
		return vol
	} else {
		return vol + "@" + cluster
	}
}

func FSPluginCommonUtilsBuildVolURI(pluginVolume string, cluster string, vol string) string {
	if cluster == "" {
		return vol
	} else {
		return pluginVolume + "@" + vol + "@" + cluster
	}
}
