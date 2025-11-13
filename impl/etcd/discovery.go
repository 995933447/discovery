package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/995933447/discovery"
	"github.com/995933447/runtimeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Service struct {
	*discovery.Service
	version int64
}

type Discovery struct {
	timeout      time.Duration
	etcd         *clientv3.Client
	srvMap       map[string]*Service
	mu           sync.RWMutex
	onSrvUpdate  discovery.OnSrvUpdatedFunc
	unwatchCh    chan struct{}
	unwatchWg    sync.WaitGroup
	KeyPrefix    string
	isWatched    bool
	logErrorFunc func(any)
}

func (d *Discovery) Unwatch() {
	d.unwatchWg.Add(1)
	d.unwatchCh <- struct{}{}
	d.unwatchWg.Wait()
}

func (d *Discovery) Unregister(ctx context.Context, srvName string, node *discovery.Node, remove bool) error {
	if srvName == "" {
		return errors.New("invalid serviceName, empty")
	}

	ctx, cancel, hasCancel := d.tryAddTimeoutToCtx(ctx)
	if hasCancel {
		defer cancel()
	}

	key := d.srvNameToEtcdKey(srvName)
	retry := 0
	maxRetry := 10
	for ; retry < maxRetry; retry++ {
		resp, err := d.etcd.Get(ctx, key)
		if err != nil {
			return err
		}

		srv := &discovery.Service{}
		if len(resp.Kvs) == 0 {
			break
		}

		err = json.Unmarshal(resp.Kvs[0].Value, srv)
		if err != nil {
			return err
		}

		existed := false
		var remainedNodes []*discovery.Node
		for _, n := range srv.Nodes {
			if n.Host != node.Host || n.Port != node.Port {
				remainedNodes = append(remainedNodes, n)
				continue
			}

			existed = true
			if remove {
				continue
			}
			n.Status = discovery.NodeStateDead
			n.Extra = node.Extra
			remainedNodes = append(remainedNodes, n)
		}

		if !existed {
			return nil
		}

		srv.Nodes = remainedNodes

		ok, err := d.atomicPersistSrv(ctx, srvName, resp.Kvs[0].ModRevision, srv)
		if err != nil {
			return err
		}

		if !ok {
			time.Sleep(time.Second)
			continue
		}

		break
	}

	if retry == maxRetry {
		return errors.New(fmt.Sprintf("set conflicted and retry fail, key %s", key))
	}

	return nil
}

func (d *Discovery) srvNameToEtcdKey(srvName string) string {
	return d.KeyPrefix + "/" + srvName
}

func (d *Discovery) UnregisterAll(ctx context.Context, srvName string) error {
	if srvName == "" {
		return errors.New("invalid serviceName, empty")
	}

	ctx, cancel, hasCancel := d.tryAddTimeoutToCtx(ctx)
	if hasCancel {
		defer cancel()
	}

	_, err := d.etcd.Delete(ctx, d.srvNameToEtcdKey(srvName))
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) watch(startRevision int64) {
	if d.isWatched {
		return
	}

	d.mu.Lock()
	if d.isWatched {
		d.mu.Unlock()
		return
	}
	d.isWatched = true
	d.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	watcher := clientv3.NewWatcher(d.etcd)
	evtCh := watcher.Watch(ctx, d.KeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(startRevision))
	lastProcessedRev := startRevision
	for {
		select {
		case <-d.unwatchCh:
			cancel()
			_ = watcher.Close()
			d.unwatchWg.Done()
			goto out
		case resp := <-evtCh:
			if resp.Canceled {
				cancel()
				_ = watcher.Close()

				d.LogError(runtimeutil.NewStackErr(fmt.Errorf("etcd watch cancel, err:%s", resp.Err())))
				time.Sleep(time.Millisecond * 50)

				ctx, cancel = context.WithCancel(context.Background())
				watcher = clientv3.NewWatcher(d.etcd)
				evtCh = watcher.Watch(ctx, d.KeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(lastProcessedRev+1))

				continue
			}

			for _, evt := range resp.Events {
				if evt.Kv.ModRevision > lastProcessedRev {
					lastProcessedRev = evt.Kv.ModRevision
				}

				key := string(evt.Kv.Key)
				if !strings.HasPrefix(key, d.KeyPrefix) {
					continue
				}

				srvName := key[len(d.KeyPrefix)+1:]

				d.mu.RLock()
				srv, ok := d.srvMap[srvName]
				d.mu.RUnlock()
				if ok {
					if srv.version > evt.Kv.ModRevision {
						continue
					}
				}

				switch evt.Type {
				case clientv3.EventTypePut:
					cfg := &discovery.Service{}
					err := json.Unmarshal(evt.Kv.Value, cfg)
					d.mu.Lock()
					if err != nil {
						delete(d.srvMap, srvName)
					} else {
						srv, ok = d.srvMap[srvName]
						if ok {
							if srv.version > evt.Kv.ModRevision {
								continue
							}
						}
						d.srvMap[srvName] = &Service{
							Service: cfg,
							version: evt.Kv.ModRevision,
						}
					}
					d.mu.Unlock()

					if d.onSrvUpdate != nil {
						d.onSrvUpdate(context.Background(), discovery.EvtUpdated, cfg)
					}
				case clientv3.EventTypeDelete:
					d.mu.Lock()
					srv, ok = d.srvMap[srvName]
					if ok {
						if srv.version > evt.Kv.ModRevision {
							d.mu.Unlock()
							continue
						}
					}
					delete(d.srvMap, srvName)
					d.mu.Unlock()
					if d.onSrvUpdate != nil {
						d.onSrvUpdate(context.Background(), discovery.EvtDeleted, &discovery.Service{
							SrvName: srvName,
						})
					}
				}
			}
		}
	}
out:
	if !d.isWatched {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.isWatched = false
	return
}

func (d *Discovery) Discover(ctx context.Context, srvName string) (*discovery.Service, error) {
	d.mu.RLock()
	srv, ok := d.srvMap[srvName]
	d.mu.RUnlock()
	if ok {
		return srv.Service, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	srv, ok = d.srvMap[srvName]
	if ok {
		return srv.Service, nil
	}

	ctx, cancel, hasCancel := d.tryAddTimeoutToCtx(ctx)
	if hasCancel {
		defer cancel()
	}

	resp, err := d.etcd.Get(ctx, d.srvNameToEtcdKey(srvName))
	if err != nil {
		return nil, err
	}

	if !d.isWatched {
		go d.watch(resp.Header.Revision + 1)
	}

	if len(resp.Kvs) == 0 {
		return nil, discovery.ErrSrvNotFound
	}

	srvCfg := &discovery.Service{}
	err = json.Unmarshal(resp.Kvs[0].Value, srvCfg)
	if err != nil {
		return nil, err
	}

	d.srvMap[srvName] = &Service{
		Service: srvCfg,
		version: resp.Kvs[0].ModRevision,
	}

	if d.onSrvUpdate != nil {
		d.onSrvUpdate(context.Background(), discovery.EvtUpdated, srvCfg)
	}

	return srvCfg, nil
}

func (d *Discovery) OnSrvUpdated(fn discovery.OnSrvUpdatedFunc) {
	d.onSrvUpdate = fn
}

func (d *Discovery) tryAddTimeoutToCtx(ctx context.Context) (triedCtx context.Context, cancel context.CancelFunc, hasCancel bool) {
	if d.timeout > 0 {
		triedCtx, cancel = context.WithTimeout(ctx, d.timeout)
		hasCancel = true
		return
	}
	triedCtx = ctx
	return
}

func (d *Discovery) LoadAll(ctx context.Context) ([]*discovery.Service, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ctx, cancel, hasCancel := d.tryAddTimeoutToCtx(ctx)
	if hasCancel {
		defer cancel()
	}

	resp, err := d.etcd.Get(ctx, d.KeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if !d.isWatched {
		go d.watch(resp.Header.Revision + 1)
	}

	var (
		services []*discovery.Service
		srvMap   = make(map[string]*Service)
	)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, d.KeyPrefix) {
			continue
		}

		srvName := key[len(d.KeyPrefix)+1:]

		srv := &discovery.Service{}
		err = json.Unmarshal(kv.Value, srv)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal service(%s)'s value failed, err:%s", srvName, err)
		}

		srvMap[srvName] = &Service{
			Service: srv,
			version: kv.ModRevision,
		}

		if d.onSrvUpdate != nil {
			d.onSrvUpdate(ctx, discovery.EvtUpdated, srv)
		}

		services = append(services, srv)
	}

	d.srvMap = srvMap

	return services, nil
}

func (d *Discovery) Register(ctx context.Context, srvName string, node *discovery.Node) error {
	if srvName == "" {
		return errors.New("invalid serviceName, empty")
	}

	if node.Host == "" || node.Port == 0 {
		return errors.New(fmt.Sprintf("invalid node, node %+v", node))
	}

	ctx, cancel, hasCancel := d.tryAddTimeoutToCtx(ctx)
	if hasCancel {
		defer cancel()
	}

	key := d.srvNameToEtcdKey(srvName)
	retry := 0
	maxRetry := 10
	for ; retry < maxRetry; retry++ {
		resp, err := d.etcd.Get(ctx, key)
		if err != nil {
			return err
		}

		srv := &discovery.Service{
			SrvName: srvName,
		}
		var srvVersion int64
		if len(resp.Kvs) > 0 {
			val := resp.Kvs[0].Value
			err = json.Unmarshal(val, srv)
			if err != nil {
				return err
			}
			srvVersion = resp.Kvs[0].ModRevision
		}

		// check node existed
		existed := false
		for _, n := range srv.Nodes {
			if n.Host != node.Host || n.Port != node.Port {
				continue
			}

			if !n.Available() {
				n.Status = discovery.NodeStateAlive
			}
			n.Extra = node.Extra

			existed = true
			break
		}
		if !existed {
			srv.Nodes = append(srv.Nodes, node)
		}

		ok, err := d.atomicPersistSrv(ctx, srvName, srvVersion, srv)
		if err != nil {
			return err
		}

		if !ok {
			time.Sleep(time.Second)
			continue
		}

		break
	}

	if retry == maxRetry {
		return errors.New(fmt.Sprintf("set conflicted and retry fail, key %s", key))
	}

	return nil
}

func (d *Discovery) atomicPersistSrv(ctx context.Context, srvName string, version int64, srv *discovery.Service) (bool, error) {
	srvJson, err := json.Marshal(srv)
	if err != nil {
		return false, err
	}

	key := d.srvNameToEtcdKey(srvName)
	tx := d.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", version))
	if len(srv.Nodes) == 0 {
		tx.Then(clientv3.OpDelete(key))
	} else {
		tx.Then(clientv3.OpPut(key, string(srvJson)))
	}
	resp, err := tx.Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

func (d *Discovery) LogError(err any) {
	if d.logErrorFunc != nil {
		d.logErrorFunc(err)
	}
}

var _ discovery.Discovery = (*Discovery)(nil)

type Options struct {
	DiscoverKeyPrefix string
	Etcd              *clientv3.Config
	DiscoverTimeout   time.Duration
	LogErrorFunc      func(any)
}

func NewDiscovery(opts *Options) (discovery.Discovery, error) {
	discoveryTimeout := opts.DiscoverTimeout
	if opts.DiscoverTimeout == 0 {
		discoveryTimeout = time.Second * 5
	}

	dis := &Discovery{
		KeyPrefix:    opts.DiscoverKeyPrefix,
		timeout:      discoveryTimeout,
		srvMap:       map[string]*Service{},
		unwatchCh:    make(chan struct{}),
		logErrorFunc: opts.LogErrorFunc,
	}

	var err error
	dis.etcd, err = clientv3.New(*opts.Etcd)
	if err != nil {
		return nil, err
	}

	return dis, nil
}
