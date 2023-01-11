package executionlayer

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	rptypes "github.com/rocket-pool/rocketpool-go/types"
)

type NotFoundError struct{}

type ForEachNodeClosure func(common.Address) bool

func (e *NotFoundError) Error() string {
	return "Key not found in cache"
}

type Cache interface {
	init() error
	getMinipoolNode(rptypes.ValidatorPubkey) (common.Address, error)
	addMinipoolNode(rptypes.ValidatorPubkey, common.Address) error
	getNodeInfo(common.Address) (*nodeInfo, error)
	addNodeInfo(common.Address, *nodeInfo) error
	forEachNode(ForEachNodeClosure) error
	addOdaoNode(common.Address) error
	removeOdaoNode(common.Address) error
	forEachOdaoNode(ForEachNodeClosure) error
	setHighestBlock(*big.Int)
	getHighestBlock() *big.Int
	deinit() error
	reset() error
}
