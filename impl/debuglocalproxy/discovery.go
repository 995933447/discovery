package debuglocalproxy

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/995933447/discovery"
	"github.com/coreos/etcd/pkg/ioutil"
	jsoniter "github.com/json-iterator/go"
)

type Discovery struct {
	conn                discovery.Discovery
	discoverLocalSrvDir string
}

func (d *Discovery) LoadAll(ctx context.Context) ([]*discovery.Service, error) {
	return d.conn.LoadAll(ctx)
}

func (d *Discovery) Register(ctx context.Context, srvName string, node *discovery.Node) error {
	if err := d.registerLocalFile(srvName, node); err != nil {
		return err
	}
	return d.conn.Register(ctx, srvName, node)
}

func (d *Discovery) Unregister(ctx context.Context, srvName string, node *discovery.Node, remove bool) error {
	if err := d.unregisterLocalFile(srvName); err != nil {
		return err
	}
	return d.conn.Unregister(ctx, srvName, node, remove)
}

func (d *Discovery) UnregisterAll(ctx context.Context, srvName string) error {
	if err := d.unregisterLocalFile(srvName); err != nil {
		return err
	}
	return d.conn.UnregisterAll(ctx, srvName)
}

func (d *Discovery) Discover(ctx context.Context, srvName string) (*discovery.Service, error) {
	srv, err := d.getSrvFromLocalFile(srvName)
	if err != nil {
		if !errors.Is(err, discovery.ErrSrvNotFound) {
			return nil, err
		}

		srv, err = d.conn.Discover(ctx, srvName)
		if err != nil {
			return nil, err
		}
	}

	return srv, nil
}

func (d *Discovery) OnSrvUpdated(updatedFunc discovery.OnSrvUpdatedFunc) {
	d.conn.OnSrvUpdated(updatedFunc)
}

func (d *Discovery) Unwatch() {
	d.conn.Unwatch()
}

func (d *Discovery) getSrvFilePath(srvName string) string {
	return d.discoverLocalSrvDir + "/" + srvName + ".local"
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

	var node discovery.Node
	if err = jsoniter.Unmarshal(buf, &node); err != nil {
		return nil, discovery.ErrSrvNotFound
	}

	return &discovery.Service{
		SrvName: srvName,
		Nodes:   []*discovery.Node{&node},
	}, nil
}

func (d *Discovery) registerLocalFile(srvName string, node *discovery.Node) error {
	path := d.getSrvFilePath(srvName)
	nodeJ, err := jsoniter.Marshal(node)
	if err != nil {
		return err
	}

	err = ioutil.WriteAndSyncFile(path, nodeJ, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) unregisterLocalFile(srvName string) error {
	return os.Remove(d.getSrvFilePath(srvName))
}

var _ discovery.Discovery = (*Discovery)(nil)

type Options struct {
	DiscoverLocalSrvDir string
	Discovery           discovery.Discovery
}

func NewDiscovery(opts *Options) discovery.Discovery {
	return &Discovery{
		discoverLocalSrvDir: opts.DiscoverLocalSrvDir,
		conn:                opts.Discovery,
	}
}
