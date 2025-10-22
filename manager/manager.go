package manager

import (
	"errors"
	"sync"

	"github.com/995933447/discovery"
)

var ErrDiscoveryNotFound = errors.New("discovery not found")

const DefaultDiscoveryName = "default"

var discoveries sync.Map

func RegisterDiscovery(name string, discovery discovery.Discovery) {
	discoveries.Store(name, discovery)
}

func RegisterDefaultDiscovery(discovery discovery.Discovery) {
	discoveries.Store(DefaultDiscoveryName, discovery)
}

func GetDiscovery(name string) (discovery.Discovery, bool) {
	dis, ok := discoveries.Load(name)
	if !ok {
		return nil, false
	}
	return dis.(discovery.Discovery), ok
}

func GetDefaultDiscovery() (discovery.Discovery, bool) {
	return GetDiscovery(DefaultDiscoveryName)
}

func MustGetDiscovery(name string) discovery.Discovery {
	if dis, ok := GetDiscovery(name); ok {
		return dis
	}

	panic(ErrDiscoveryNotFound)
}

func MustGetDefaultDiscovery() discovery.Discovery {
	return MustGetDiscovery(DefaultDiscoveryName)
}
