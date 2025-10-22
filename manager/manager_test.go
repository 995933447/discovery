package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/995933447/discovery"
	"github.com/995933447/discovery/impl/debuglocalproxy"
	"github.com/995933447/discovery/impl/etcd"
	"github.com/995933447/discovery/impl/filecacheproxy"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestDebugLocalDiscovery(t *testing.T) {
	dis, err := NewDebugLocalDiscovery()
	if err != nil {
		t.Fatal(err)
	}

	node := discovery.NewNode("127.0.0.1", 666)
	err = dis.Register(context.TODO(), "user", node)
	if err != nil {
		t.Fatal(err)
	}

	srv, err := dis.Discover(context.TODO(), "user")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(srv)

	dis.Unregister(context.TODO(), "user", node, true)
}

func TestFileCacheDiscovery(t *testing.T) {
	for i := 0; i < 50; i++ {
		ii := i
		go func() {
			if ii == 0 {
				RunWithFileCache(t, 666+ii)
				return
			}
			RunWithFileCache(t, 666+ii)
		}()
	}

	select {}
}

func RunWithFileCache(t *testing.T, port int) {
	dis, err := NewFileCacheDiscovery()
	if err != nil {
		t.Fatal(err)
	}

	node := discovery.NewNode("127.0.0.1", port)

	err = dis.Register(context.TODO(), "member", node)
	if err != nil {
		t.Error(err)
	}

	defer func() {
		err := dis.Unregister(context.TODO(), "member", node, true)
		if err != nil {
			t.Fatal(err)
		}
		dis.Unwatch()
		time.Sleep(1 * time.Second)
	}()

	nodes, err := dis.Discover(context.Background(), "member")
	if err != nil {
		t.Error(err)
	}

	t.Log(nodes)

	services, err := dis.LoadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(services)

	time.Sleep(10 * time.Second)
}

func InitFileCacheDiscovery() error {
	fileCacheDiscovery, err := NewFileCacheDiscovery()
	if err != nil {
		return err
	}

	RegisterDefaultDiscovery(fileCacheDiscovery)

	return nil
}

func NewFileCacheDiscovery() (discovery.Discovery, error) {
	etcdDiscovery, err := etcd.NewDiscovery(&etcd.Options{
		DiscoverKeyPrefix: "test_discovery_factory_",
		DiscoverTimeout:   time.Minute * 10,
		Etcd: &clientv3.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
	})
	if err != nil {
		return nil, err
	}

	withFileCacheDiscovery, err := filecacheproxy.NewDiscovery(&filecacheproxy.Options{
		Dir:       "/var/work/discovery",
		Discovery: etcdDiscovery,
		LogErrFunc: func(err any) {
			fmt.Println(err)
		},
	})
	if err != nil {
		return nil, err
	}

	return withFileCacheDiscovery, nil
}

func NewDebugLocalDiscovery() (discovery.Discovery, error) {
	dis, err := NewFileCacheDiscovery()
	if err != nil {
		return nil, err
	}

	return debuglocalproxy.NewDiscovery(&debuglocalproxy.Options{
		DiscoverLocalSrvDir: "/var/work/discovery",
		Discovery:           dis,
	}), nil
}
