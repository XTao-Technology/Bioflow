package config

import (
    "strings"
    "time"
    etcdclient "github.com/coreos/etcd/client"
    "golang.org/x/net/context"

    . "github.com/xtao/xstone/common"
    )

type ETCDConfDB struct {
    etcdClient etcdclient.Client
    keysAPI etcdclient.KeysAPI
}

func NewETCDConfDB(endpoints []string) *ETCDConfDB {
    cfg := etcdclient.Config{
            Endpoints: endpoints,
    }

    c, err := etcdclient.New(cfg)
    if err != nil {
        StoneLogger.Errorf("Fail to initialize etcd client: %s\n",
            err.Error())
        return nil
    }
    kAPI := etcdclient.NewKeysAPI(c)

    db := &ETCDConfDB{
                etcdClient: c,
                keysAPI: kAPI,
            }

    return db
}

func (db *ETCDConfDB)GetSingleKey(key string) (error, []byte) {
    ctx := context.Background()
    sopt := etcdclient.GetOptions {
                Recursive: false,
                Sort: false,
                Quorum: false,
            }

    res, err := db.keysAPI.Get(ctx, key, &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't get single key %s: %s\n",
            key, err.Error())
        return err, nil
    }

    return nil, []byte(res.Node.Value)
}

func (db *ETCDConfDB)GetKeysOfDirectory(dirKey string) (error, map[string][]byte) {
    ctx := context.Background()
    sopt := etcdclient.GetOptions {
                Recursive: true,
                Sort: false,
                Quorum: false,
            }

    res, err := db.keysAPI.Get(ctx, dirKey, &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't get single key %s: %s\n",
            dirKey, err.Error())
        return err, nil
    }
    
    if res.Node.Nodes != nil {
        subKeys := make(map[string][]byte)
        for _, node := range res.Node.Nodes {
            keyItems := strings.Split(node.Key, "/")
            key := keyItems[len(keyItems) - 1]
            subKeys[key] = []byte(node.Value)
        }
        return nil, subKeys
    } else {
        return nil, nil
    }
}

func (db *ETCDConfDB)SetSingleKey(key string, value string, ttl time.Duration) error {
    ctx := context.Background()
    sopt := etcdclient.SetOptions {
                Refresh: false,
                PrevExist: etcdclient.PrevIgnore,
                Dir: false,
                TTL: ttl,
            }
    _, err := db.keysAPI.Set(ctx, key, value, &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't set single key %s: %s\n",
            key, err.Error())
        return err
    }

    return nil
}

func (db *ETCDConfDB)DeleteSingleKey(key string) error {
    ctx := context.Background()
    dopt := etcdclient.DeleteOptions {}
    _, err := db.keysAPI.Delete(ctx, key, &dopt)
    if err != nil {
        StoneLogger.Errorf("Can't delete single key %s: %s\n",
            key, err.Error())
        return err
    }

    return nil
}

func (db *ETCDConfDB)RefreshKey(key string, ttl time.Duration) error {
    ctx := context.Background()
    sopt := etcdclient.SetOptions {
                Refresh: true,
                TTL: ttl,
            }
    _, err := db.keysAPI.Set(ctx, key, "", &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't refresh the key %s: %s\n",
            key, err.Error())
        return err
    }

    return nil
}

func (db *ETCDConfDB)CompareAndSetKey(key string, newValue string, oldValue string) error {
    ctx := context.Background()
    sopt := etcdclient.SetOptions {
                PrevValue: oldValue,
                PrevExist: etcdclient.PrevIgnore,
                Dir: false,
                Refresh: false,
            }
    _, err := db.keysAPI.Set(ctx, key, newValue, &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't compare and set key %s: %s\n",
            key, err.Error())
        return err
    }

    return nil
}

func (db *ETCDConfDB)SetKeyIfNotExist(key string, newValue string) error {
    ctx := context.Background()
    sopt := etcdclient.SetOptions {
                PrevExist: etcdclient.PrevNoExist,
                Dir: false,
            }
    _, err := db.keysAPI.Set(ctx, key, newValue, &sopt)
    if err != nil {
        StoneLogger.Errorf("Can't set not exist key %s: %s\n",
            key, err.Error())
        return err
    }

    return nil
}

func (db *ETCDConfDB)NewRecursiveWatcher(key string) *BaseConfWatcher {
    wopt := etcdclient.WatcherOptions {
                        Recursive: true,
                    }
    ctx := context.Background()

    return &BaseConfWatcher{
            watcher: db.keysAPI.Watcher(key, &wopt),
            ctxt: ctx,
            key: key,
            db: db,
    }
}

