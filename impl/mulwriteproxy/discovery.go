package mulwriteproxy

import (
	"context"

	"github.com/995933447/discovery"
)

type Discovery struct {
	mainDiscovery                      discovery.Discovery
	otherDiscoveries                   []discovery.Discovery
	enabledMulRegisterOnSrvUpdatedFunc bool
}

func (d *Discovery) LoadAll(ctx context.Context) ([]*discovery.Service, error) {
	return d.mainDiscovery.LoadAll(ctx)
}

func (d *Discovery) Register(ctx context.Context, srvName string, node *discovery.Node) error {
	if err := d.mainDiscovery.Register(ctx, srvName, node); err != nil {
		return err
	}

	for _, dis := range d.otherDiscoveries {
		if err := dis.Register(ctx, srvName, node); err != nil {
			return err
		}
	}

	return nil
}

func (d *Discovery) Unregister(ctx context.Context, srvName string, node *discovery.Node, remove bool) error {
	if err := d.mainDiscovery.Unregister(ctx, srvName, node, remove); err != nil {
		return err
	}

	for _, dis := range d.otherDiscoveries {
		if err := dis.Unregister(ctx, srvName, node, remove); err != nil {
			return err
		}
	}

	return nil
}

func (d *Discovery) UnregisterAll(ctx context.Context, srvName string) error {
	if err := d.mainDiscovery.UnregisterAll(ctx, srvName); err != nil {
		return err
	}

	for _, dis := range d.otherDiscoveries {
		if err := dis.UnregisterAll(ctx, srvName); err != nil {
			return err
		}
	}

	return nil
}

func (d *Discovery) Discover(ctx context.Context, srvName string) (*discovery.Service, error) {
	return d.mainDiscovery.Discover(ctx, srvName)
}

func (d *Discovery) OnSrvUpdated(updatedFunc discovery.OnSrvUpdatedFunc) {
	d.mainDiscovery.OnSrvUpdated(updatedFunc)
	if d.enabledMulRegisterOnSrvUpdatedFunc {
		for _, dis := range d.otherDiscoveries {
			dis.OnSrvUpdated(updatedFunc)
		}
	}
}

func (d *Discovery) Unwatch() {
	d.mainDiscovery.Unwatch()
	for _, dis := range d.otherDiscoveries {
		dis.Unwatch()
	}
}

var _ discovery.Discovery = (*Discovery)(nil)

type Options struct {
	MainDiscovery                      discovery.Discovery
	OtherDiscoveries                   []discovery.Discovery
	EnabledMulRegisterOnSrvUpdatedFunc bool
}

func NewDiscovery(opts *Options) *Discovery {
	return &Discovery{
		mainDiscovery:                      opts.MainDiscovery,
		otherDiscoveries:                   opts.OtherDiscoveries,
		enabledMulRegisterOnSrvUpdatedFunc: opts.EnabledMulRegisterOnSrvUpdatedFunc,
	}
}
