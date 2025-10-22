package filecacheproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/995933447/discovery"
	"github.com/995933447/runtimeutil"
	"github.com/coreos/etcd/pkg/ioutil"
	"github.com/fsnotify/fsnotify"
	"github.com/gofrs/flock"
	jsoniter "github.com/json-iterator/go"
)

const (
	DiscoverCacheFileSuffix = ".json"
)

type Discovery struct {
	dir                string
	conn               discovery.Discovery
	mu                 sync.RWMutex
	srvMap             map[string]*discovery.Service
	onSrvUpdate        discovery.OnSrvUpdatedFunc
	unwatchCh          chan struct{}
	unwatchWg          sync.WaitGroup
	isWatched          bool
	logErrorFunc       func(any)
	isMaster           bool
	raceMasterLocker   *flock.Flock
	srvFileLocks       sync.Map
	refreshedFileCache bool
	loadedAll          bool
}

func (d *Discovery) LoadAll(ctx context.Context) ([]*discovery.Service, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.isWatched {
		if err := d.watch(); err != nil {
			return nil, err
		}
	}

	if d.isMaster {
		services, err := d.conn.LoadAll(ctx)
		if err != nil {
			return nil, err
		}

		srvMap := make(map[string]*discovery.Service)
		for _, srv := range services {
			srvMap[srv.SrvName] = srv
		}

		// 首次启动删除可能被删除的服务缓存文件
		if !d.refreshedFileCache {
			files, err := os.ReadDir(d.dir)
			if err != nil {
				return nil, err
			}

			for _, file := range files {
				if file.IsDir() {
					continue
				}

				fileName := file.Name()
				if !strings.HasSuffix(fileName, ".json") {
					continue
				}

				suffixPos := strings.IndexByte(fileName, '.')
				if suffixPos <= 0 {
					continue
				}

				srvName := fileName[:suffixPos]

				if _, ok := srvMap[srvName]; ok {
					continue
				}

				if err = os.Remove(d.getSrvFilePath(srvName)); err != nil {
					if os.IsNotExist(err) {
						continue
					}
					return nil, err
				}
			}

			// 再次刷新触发OnSrvUpdated更新缓存文件
			services, err = d.conn.LoadAll(ctx)
			if err != nil {
				return nil, err
			}

			srvMap = make(map[string]*discovery.Service)
			for _, srv := range services {
				srvMap[srv.SrvName] = srv
			}

			d.refreshedFileCache = true
		}

		d.loadedAll = true
		d.srvMap = srvMap

		return services, nil
	}

	files, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}

	var (
		services []*discovery.Service
		srvMap   = make(map[string]*discovery.Service)
	)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		if !strings.HasSuffix(fileName, ".json") {
			continue
		}

		suffixPos := strings.IndexByte(fileName, '.')
		if suffixPos <= 0 {
			continue
		}

		srvName := fileName[:suffixPos]

		srv, err := d.getSrvFromLocalFile(srvName)
		if err != nil {
			if errors.Is(err, discovery.ErrSrvNotFound) {
				continue
			}
			return nil, err
		}

		srvMap[srvName] = srv
		services = append(services, srv)
	}

	d.loadedAll = true
	d.srvMap = srvMap

	return services, nil
}

func (d *Discovery) Register(ctx context.Context, srvName string, node *discovery.Node) error {
	return d.conn.Register(ctx, srvName, node)
}

func (d *Discovery) Unregister(ctx context.Context, srvName string, node *discovery.Node, remove bool) error {
	return d.conn.Unregister(ctx, srvName, node, remove)
}

func (d *Discovery) UnregisterAll(ctx context.Context, srvName string) error {
	return d.conn.UnregisterAll(ctx, srvName)
}

func (d *Discovery) Discover(ctx context.Context, srvName string) (*discovery.Service, error) {
	d.mu.RLock()
	srv, ok := d.srvMap[srvName]
	d.mu.RUnlock()
	if ok {
		return srv, nil
	}

	if d.loadedAll {
		return nil, discovery.ErrSrvNotFound
	}

	d.mu.Lock()
	srv, ok = d.srvMap[srvName]
	if ok {
		d.mu.Unlock()
		return srv, nil
	}

	if d.loadedAll {
		d.mu.Unlock()
		return nil, discovery.ErrSrvNotFound
	}

	if !d.isWatched {
		if err := d.watch(); err != nil {
			d.mu.Unlock()
			return nil, err
		}
	}

	srv, err := d.getSrvFromLocalFile(srvName)
	if err != nil {
		d.mu.Unlock()
		return nil, err
	}

	d.srvMap[srvName] = srv
	d.mu.Unlock()

	if d.onSrvUpdate != nil {
		d.onSrvUpdate(ctx, discovery.EvtUpdated, srv)
	}

	return srv, nil
}

func (d *Discovery) OnSrvUpdated(fn discovery.OnSrvUpdatedFunc) {
	d.onSrvUpdate = fn
}

func (d *Discovery) watch() error {
	if d.isWatched {
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	if err = watcher.Add(d.dir); err != nil {
		if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no such file or directory") {
			return err
		}

		if err = os.MkdirAll(d.dir, os.ModePerm); err != nil {
			return err
		}

		for i := 0; i < 3; i++ {
			if err = watcher.Add(d.dir); err != nil {
				if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no such file or directory") {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
	}

	d.isWatched = true

	go func() {
		syncSrvTk := time.NewTicker(time.Second)
		defer syncSrvTk.Stop()

		var mapSrvToUpdatedAt sync.Map

		for {
			select {
			case <-d.unwatchCh:
				if err := watcher.Close(); err != nil {
					d.LogError(runtimeutil.NewStackErr(err))
				}

				d.mu.Lock()
				d.isWatched = false
				d.mu.Unlock()

				if d.isMaster {
					d.conn.Unwatch()
				}

				d.unwatchWg.Done()
				return
			case <-syncSrvTk.C:
				var mapSrvToUpdatedAtCp sync.Map

				mapSrvToUpdatedAt.Range(func(key, value interface{}) bool {
					mapSrvToUpdatedAtCp.Store(key, value)
					return true
				})

				mapSrvToUpdatedAtCp.Range(func(key, value interface{}) bool {
					srv := key.(string)
					updatedAt := value.(time.Time)

					if time.Since(updatedAt) < time.Second {
						return true
					}

					d.mu.Lock()
					defer d.mu.Unlock()

					cfg, err := d.getSrvFromLocalFile(srv)
					if err != nil {
						if !errors.Is(err, discovery.ErrSrvNotFound) {
							d.LogError(runtimeutil.NewStackErr(err))
							return true
						}

						delete(d.srvMap, srv)
						return true
					}

					d.srvMap[srv] = cfg
					return true
				})
			case evt, ok := <-watcher.Events:
				if !ok {
					d.LogError(runtimeutil.NewStackErr(errors.New("watch interrupted")))

					select {
					case err := <-watcher.Errors:
						d.LogError(runtimeutil.NewStackErr(err))
					default:
					}

					_ = watcher.Close()
					watcher = nil

					var watchSuccess bool
					for i := 0; i < 10; i++ {
						if i > 0 {
							time.Sleep(time.Second * time.Duration(i))
						}

						if watcher == nil {
							watcher, err = fsnotify.NewWatcher()
							if err != nil {
								d.LogError(runtimeutil.NewStackErr(err))
								continue
							}
						}

						if err = watcher.Add(d.dir); err != nil {
							if !os.IsNotExist(err) {
								d.LogError(runtimeutil.NewStackErr(err))
								continue
							}

							if err = os.MkdirAll(d.dir, os.ModePerm); err != nil {
								d.LogError(runtimeutil.NewStackErr(err))
								continue
							}

							if err = watcher.Add(d.dir); err != nil {
								d.LogError(runtimeutil.NewStackErr(err))
								continue
							}
						}

						watchSuccess = true
						break
					}

					if !watchSuccess {
						d.LogError(runtimeutil.NewStackErr(fmt.Errorf("emergency error: watcher failed (T . T)")))

						d.mu.Lock()
						d.isWatched = false
						d.mu.Unlock()

						return
					}

					continue
				}

				if evt.Op&fsnotify.Chmod != 0 {
					continue
				}

				if !strings.HasSuffix(evt.Name, DiscoverCacheFileSuffix) {
					continue
				}

				srv := strings.TrimSuffix(path.Base(evt.Name), DiscoverCacheFileSuffix)
				mapSrvToUpdatedAt.Store(srv, time.Now())
			}
		}
	}()

	return nil
}

func (d *Discovery) Unwatch() {
	d.unwatchWg.Add(1)
	d.unwatchCh <- struct{}{}
	d.unwatchWg.Wait()
}

func (d *Discovery) getSrvFilePath(srvName string) string {
	return d.dir + "/" + srvName + DiscoverCacheFileSuffix
}

func (d *Discovery) getSrvFromLocalFile(srvName string) (*discovery.Service, error) {
	filePath := d.getSrvFilePath(srvName)
	fp, err := os.Open(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return nil, discovery.ErrSrvNotFound
	}
	defer fp.Close()
	buf, err := io.ReadAll(fp)
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, discovery.ErrSrvNotFound
	}
	var srv discovery.Service
	err = jsoniter.Unmarshal(buf, &srv)
	if err != nil {
		return nil, err
	}
	return &srv, nil
}

// 实际上一般情况下不需要显示调用放弃master释放文件锁,因为文件锁是以文件描述符加锁的,进程退出文件描述符就被清理了,文件锁就自动释放了
func (d *Discovery) abandonMaster() error {
	d.isMaster = false
	if err := d.raceMasterLocker.Unlock(); err != nil {
		d.LogError(runtimeutil.NewStackErr(err))
		return err
	} else {
		filePath := d.getMasterLockFilePath()
		if err = ioutil.WriteAndSyncFile(filePath, []byte(""), 0600); err != nil {
			d.LogError(runtimeutil.NewStackErr(err))
			return err
		}
	}
	return nil
}

func (d *Discovery) SyncSrvToLocalFile(srv *discovery.Service) error {
	l, _ := d.srvFileLocks.LoadOrStore(srv.SrvName, &sync.Mutex{})
	locker := l.(*sync.Mutex)
	locker.Lock()
	defer locker.Unlock()

	path := d.getSrvFilePath(srv.SrvName)
	tmpPath := path + ".tmp"

	data, err := jsoniter.Marshal(srv)
	if err != nil {
		return err
	}

	// 写入临时文件
	if err := ioutil.WriteAndSyncFile(tmpPath, data, 0666); err != nil {
		return err
	}

	// 原子重命名覆盖正式文件
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	return nil
}

func (d *Discovery) getMasterLockFilePath() string {
	return d.dir + "/master.pid"
}

func (d *Discovery) raceMaster() error {
	filePath := d.getMasterLockFilePath()

	d.mu.Lock()
	if d.raceMasterLocker == nil {
		d.raceMasterLocker = flock.New(filePath)
	}
	d.mu.Unlock()

	err := d.raceMasterLocker.Lock()
	if err != nil {
		return err
	}

	d.isMaster = true

	err = ioutil.WriteAndSyncFile(filePath, []byte(fmt.Sprintf("%d", os.Getpid())), 0666)
	if err != nil {
		d.LogError(runtimeutil.NewStackErr(err))
	}

	if _, err = d.LoadAll(context.Background()); err != nil {
		return err
	}

	return nil
}

func (d *Discovery) tryBeMaster() (bool, error) {
	filePath := d.getMasterLockFilePath()

	d.mu.Lock()
	if d.raceMasterLocker == nil {
		d.raceMasterLocker = flock.New(filePath)
	}
	d.mu.Unlock()

	ok, err := d.raceMasterLocker.TryLock()
	if err != nil {
		return false, err
	}

	d.isMaster = ok

	if d.isMaster {
		err = ioutil.WriteAndSyncFile(filePath, []byte(fmt.Sprintf("%d", os.Getpid())), 0666)
		if err != nil {
			d.LogError(runtimeutil.NewStackErr(err))
		}

		if _, err = d.LoadAll(context.Background()); err != nil {
			return false, err
		}
	}

	return d.isMaster, nil
}

func (d *Discovery) stillTryBeMaster() (bool, error) {
	ok, err := d.tryBeMaster()
	if err != nil {
		return false, err
	}

	if !ok {
		go func() {
			for {
				if err := d.raceMaster(); err != nil {
					d.LogError(runtimeutil.NewStackErr(err))
				} else {
					break
				}
			}
		}()
	}

	return ok, nil
}

func (d *Discovery) LogError(err any) {
	if d.logErrorFunc != nil {
		d.logErrorFunc(err)
	}
}

var _ discovery.Discovery = (*Discovery)(nil)

type Options struct {
	Dir        string `json:"dir"`
	Discovery  discovery.Discovery
	LogErrFunc func(any)
}

func NewDiscovery(opts *Options) (discovery.Discovery, error) {
	dis := &Discovery{
		dir:          opts.Dir,
		conn:         opts.Discovery,
		srvMap:       map[string]*discovery.Service{},
		unwatchCh:    make(chan struct{}),
		logErrorFunc: opts.LogErrFunc,
	}

	dis.dir = strings.TrimRight(dis.dir, "/")

	opts.Discovery.OnSrvUpdated(func(ctx context.Context, evt discovery.Evt, srv *discovery.Service) {
		if !dis.isMaster {
			return
		}

		switch evt {
		case discovery.EvtUpdated:
			if err := dis.SyncSrvToLocalFile(srv); err != nil {
				dis.LogError(runtimeutil.NewStackErr(err))
			}
		case discovery.EvtDeleted:
			err := os.Remove(dis.getSrvFilePath(srv.SrvName))
			if err != nil {
				dis.LogError(runtimeutil.NewStackErr(err))
			}
		default:
		}
	})

	if _, err := dis.stillTryBeMaster(); err != nil {
		return nil, err
	}

	return dis, nil
}
