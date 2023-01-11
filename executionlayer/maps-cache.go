package executionlayer

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	rptypes "github.com/rocket-pool/rocketpool-go/types"
)

type MapsCache struct {
	// A long-lived index of pubkey->node address
	//
	// If a guarded query contains a pubkey we've seen before, and the fee recipient is
	// the smoothing pool, no further validation is needed, so we can exit early based
	// on membership in this map.
	//
	// Since this index is expected to strictly grow, we can use sync.Map to deal with
	// concurrent access. Elements are only inserted, never deleted.
	minipoolIndex *sync.Map

	// We need to store each node's smoothing pool status and fee recipient address.
	// We will subscribe to rocketNodeManager's events stream, which will notify us of
	// changes- to keep map contention down, we will use pointers as elements.
	// Ergo, this is a map of node address -> *Node
	nodeIndex *sync.Map

	// We store oDAO nodes for the api. We need to be able to remove them if they're
	// kicked or leave, so this is a map of address -> bool
	odaoNodeIndex *sync.Map

	// We need to detect gaps in the event stream when there are connection issues, and
	// backfill missing data, so we keep track of the highest block for which we received
	// an event here.
	highestBlock *big.Int
}

func (m *MapsCache) init() error {

	m.minipoolIndex = &sync.Map{}
	m.nodeIndex = &sync.Map{}
	m.odaoNodeIndex = &sync.Map{}
	m.highestBlock = big.NewInt(0)
	return nil
}

func (m *MapsCache) getMinipoolNode(pubkey rptypes.ValidatorPubkey) (common.Address, error) {

	void, ok := m.minipoolIndex.Load(pubkey)
	if !ok {
		return common.Address{}, &NotFoundError{}
	}

	nodeAddr, ok := void.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("could not convert cache result into common.address")
	}

	return nodeAddr, nil
}

func (m *MapsCache) addMinipoolNode(pubkey rptypes.ValidatorPubkey, nodeAddr common.Address) error {

	m.minipoolIndex.Store(pubkey, nodeAddr)
	return nil
}

func (m *MapsCache) getNodeInfo(nodeAddr common.Address) (*nodeInfo, error) {

	void, ok := m.nodeIndex.Load(nodeAddr)
	if !ok {
		return nil, &NotFoundError{}
	}

	nodeInfo, ok := void.(*nodeInfo)
	if !ok {
		return nil, fmt.Errorf("could not convert cache result into *nodeInfo")
	}

	return nodeInfo, nil
}

func (m *MapsCache) addNodeInfo(nodeAddr common.Address, node *nodeInfo) error {

	m.nodeIndex.Store(nodeAddr, node)
	return nil
}

func (m *MapsCache) forEachNode(closure ForEachNodeClosure) error {
	m.nodeIndex.Range(func(k any, value any) bool {
		return closure(k.(common.Address))
	})

	return nil
}

func (m *MapsCache) addOdaoNode(addr common.Address) error {

	m.odaoNodeIndex.Store(addr, true)
	return nil
}

func (m *MapsCache) removeOdaoNode(addr common.Address) error {

	m.odaoNodeIndex.Store(addr, false)
	return nil
}

func (m *MapsCache) forEachOdaoNode(closure ForEachNodeClosure) error {
	m.odaoNodeIndex.Range(func(k any, value any) bool {
		if !value.(bool) {
			return true
		}

		return closure(k.(common.Address))
	})

	return nil
}

func (m *MapsCache) setHighestBlock(block *big.Int) {
	if m.highestBlock.Cmp(block) >= 0 {
		return
	}

	// Someone else owns this pointer, so make a new one
	m.highestBlock = big.NewInt(0)
	m.highestBlock.Add(block, m.highestBlock)
}

func (m *MapsCache) getHighestBlock() *big.Int {

	return m.highestBlock
}

func (m *MapsCache) deinit() error {
	return nil
}

func (m *MapsCache) reset() error {
	return m.init()
}
