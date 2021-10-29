package common

import (
    "net"
    "net/url"
    "time"
    "fmt"
    )

/*
 * It caches the dns name <-> ip mapping in a reasonable
 * time. When time expired, it refresh it by a DNS lookup.
 * this is to minimize the pressure of DNS server because
 * the name ip mapping should be changed in most of time.
 */
type NameIPCache struct {
    url string
    name string
    port string
    ip string

    lastResolve time.Time
    timeout float64
    expired bool
}

/*
 * instantiate a name ip cache with epoints URL, which is
 * name:port pair
 */
func NewURLNameIPCache(urlEndPoint string, timeout float64) *NameIPCache {
    url, err := url.Parse(urlEndPoint)
    if err != nil {
        StoneLogger.Errorf("Can't parse url %s to init name cache: %s\n",
            urlEndPoint, err.Error())
        return nil
    }
    host, port, err := net.SplitHostPort(url.Host)
    if err != nil {
        StoneLogger.Errorf("Fail to get host port from %v to init name cache:%s\n",
            url.Host, err.Error())
        return nil
    }
    nameCache := &NameIPCache{
                    url: urlEndPoint,
                    name: host,
                    port: port,
                    ip: "",
                    timeout: timeout,
                    expired: true,
    }
    nameCache.Initialize()

    StoneLogger.Infof("Created url name cache %s %s:%s\n",
        urlEndPoint, host, port)

    return nameCache
}

func (cache *NameIPCache) Initialize() {
    go func() {
        for true {
            for cache.expired {
                ips, err := net.LookupIP(cache.name)
                if err != nil {
                    StoneLogger.Errorf("Can't resolve %s: %s\n",
                        cache.name, err.Error())
                    time.Sleep(20 * time.Second)
                } else if ips == nil || len(ips) == 0 {
                    StoneLogger.Errorf("No ip resolved for %s\n",
                        cache.name)
                    time.Sleep(20 * time.Second)
                } else {
                    cache.ip = ips[0].String()
                    cache.expired = false
                    cache.lastResolve = time.Now()
                }
            }

            /*wait a period*/
            select {
                case <- time.After(time.Duration(cache.timeout) * time.Second):
                    cache.expired = true
            }
        }
    }()
}

func (cache *NameIPCache) GetURL() string {
    if cache.ip == "" {
        return cache.url
    } else {
        return fmt.Sprintf("http://%s:%s", cache.ip, cache.port)
    }
}
