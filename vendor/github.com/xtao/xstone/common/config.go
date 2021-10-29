package common

type RestServerConfig struct {
	Server string
	Port   string
}

type XstoneConfig struct{
	restServerConfig RestServerConfig
}