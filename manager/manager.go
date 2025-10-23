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

func GetDiscovery(name string) (discovery.Discovery, error) {
	dis, ok := discoveries.Load(name)
	if !ok {
		return nil, ErrDiscoveryNotFound
	}
	return dis.(discovery.Discovery), nil
}

func GetDefaultDiscovery() (discovery.Discovery, error) {
	return GetDiscovery(DefaultDiscoveryName)
}

func MustGetDiscovery(name string) discovery.Discovery {
	dis, err := GetDiscovery(name)
	if err != nil {
		panic(err)
	}

	return dis
}

func MustGetDefaultDiscovery() discovery.Discovery {
	return MustGetDiscovery(DefaultDiscoveryName)
}
