package executionlayer

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"

	"github.com/Rocket-Pool-Rescue-Node/rescue-proxy/metrics"
	"github.com/ethereum/go-ethereum/common"
	driver "github.com/mattn/go-sqlite3"
	rptypes "github.com/rocket-pool/rocketpool-go/types"
)

type SqliteCache struct {
	Path                string
	db                  *sql.DB
	getMinipoolStmt     *sql.Stmt
	getNodeStmt         *sql.Stmt
	getHighestBlockStmt *sql.Stmt
	setMinipoolStmt     *sql.Stmt
	setNodeStmt         *sql.Stmt
	setHighestBlockStmt *sql.Stmt
	forEachNodeStmt     *sql.Stmt
	addOdaoNodeStmt     *sql.Stmt
	delOdaoNodeStmt     *sql.Stmt
	forEachOdaoNodeStmt *sql.Stmt

	// Track the highest block in memory and save to db before serializing
	highestBlock *big.Int

	m *metrics.MetricsRegistry
}

const snapshotFileName = "rescue-proxy-cache.sql"

func (s *SqliteCache) prepareStatements() error {
	var err error

	s.getMinipoolStmt, err = s.db.Prepare("SELECT node_address FROM minipools WHERE pubkey = ?;")
	if err != nil {
		return err
	}
	s.getNodeStmt, err = s.db.Prepare("SELECT smoothing_pool_status, fee_distributor FROM nodes WHERE address = ?;")
	if err != nil {
		return err
	}
	s.getHighestBlockStmt, err = s.db.Prepare("SELECT value FROM highest_block WHERE id = 0;")
	if err != nil {
		return err
	}

	s.setMinipoolStmt, err = s.db.Prepare("INSERT OR REPLACE INTO minipools(pubkey, node_address) VALUES( ?, ?);")
	if err != nil {
		return err
	}
	s.setNodeStmt, err = s.db.Prepare("INSERT OR REPLACE INTO nodes(address, smoothing_pool_status, fee_distributor) VALUES( ?, ?, ?);")
	if err != nil {
		return err
	}
	s.setHighestBlockStmt, err = s.db.Prepare("INSERT OR REPLACE INTO highest_block(id, value) VALUES(0, ?);")
	if err != nil {
		return err
	}

	s.forEachNodeStmt, err = s.db.Prepare("SELECT address FROM nodes;")
	if err != nil {
		return err
	}

	s.addOdaoNodeStmt, err = s.db.Prepare("INSERT OR REPLACE INTO odao_nodes(address) VALUES ( ?);")
	if err != nil {
		return err
	}

	s.delOdaoNodeStmt, err = s.db.Prepare("DELETE FROM odao_nodes WHERE address = ( ?);")
	if err != nil {
		return err
	}

	s.forEachOdaoNodeStmt, err = s.db.Prepare("SELECT address FROM odao_nodes;")
	if err != nil {
		return err
	}

	return nil
}

func (s *SqliteCache) createTables() error {
	const nodes string = `
		CREATE TABLE IF NOT EXISTS nodes (
			address BLOB PRIMARY KEY,
			smoothing_pool_status TINYINT,
			fee_distributor BLOB
		);`

	const minipools string = `
		CREATE TABLE IF NOT EXISTS minipools (
			pubkey BLOB PRIMARY KEY,
			node_address BLOB
		);`

	const highestBlock string = `
		CREATE TABLE IF NOT EXISTS highest_block (
			id INTEGER PRIMARY KEY CHECK (id = 0),
			value INTEGER(8)
		);`

	const odaoNodes string = `
		CREATE TABLE IF NOT EXISTS odao_nodes (
			address BLOB PRIMARY KEY
		);`

	if _, err := s.db.Exec(nodes); err != nil {
		return err
	}

	if _, err := s.db.Exec(minipools); err != nil {
		return err
	}

	if _, err := s.db.Exec(highestBlock); err != nil {
		return err
	}

	if _, err := s.db.Exec(odaoNodes); err != nil {
		return err
	}

	return nil

}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}

func cloneSqlDB(dst, src *sql.DB) error {

	srcConn, err := src.Conn(context.Background())
	if err != nil {
		return err
	}

	dstConn, err := dst.Conn(context.Background())
	if err != nil {
		return err
	}

	err = dstConn.Raw(func(dstDConn any) error {
		dstSQLiteConn, ok := dstDConn.(*driver.SQLiteConn)
		if !ok {
			return fmt.Errorf("failed to cast connection to sqlite")
		}

		return srcConn.Raw(func(srcDConn any) error {
			srcSQLiteConn, ok := srcDConn.(*driver.SQLiteConn)
			if !ok {
				return fmt.Errorf("failed to cast connection to sqlite")
			}

			backup, err := dstSQLiteConn.Backup("main", srcSQLiteConn, "main")
			if err != nil {
				return err
			}

			done, err := backup.Step(-1)
			if !done {
				return fmt.Errorf("couldn't clone db in a single pass")
			}
			if err != nil {
				return err
			}

			err = backup.Finish()
			return err
		})
	})

	return err
}

func (s *SqliteCache) init() error {
	var err error

	s.m = metrics.NewMetricsRegistry("sqlite_cache")

	// Set highestBlock to 0. We can load it from the snapshot later
	s.highestBlock = big.NewInt(0)

	s.db, err = sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		return err
	}

	// Check if the path exists
	if _, err = os.Stat(s.Path); os.IsNotExist(err) {
		// Create the path
		err = os.MkdirAll(s.Path, 0600)
	}

	if err != nil {
		return err
	}

	// See if we have a sqlite snapshot we can load
	if _, err = os.Stat(s.Path + "/" + snapshotFileName); os.IsNotExist(err) {
		// Nothing to load, so initalize tables
		goto cont
	}

	{
		snapshotPath := s.Path + "/" + snapshotFileName

		if err != nil {
			return err
		}

		// A snapshot exists, so load it now
		src, err := sql.Open("sqlite3", "file:"+snapshotPath)
		if err != nil {
			return err
		}
		defer src.Close()

		err = cloneSqlDB(s.db, src)
		if err != nil {
			return err
		}
	}
cont:
	err = s.createTables()
	if err != nil {
		return err
	}

	err = s.prepareStatements()
	if err != nil {
		return err
	}

	// Finally, grab the highest block from the db
	// If there was no snapshot, this will not return anything,
	// and the user will know they need to warm up the cache

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	rows, err := tx.Stmt(s.getHighestBlockStmt).Query()
	if err != nil {
		return err
	}

	if !rows.Next() {
		// No block in the db
		return tx.Commit()
	}

	var block int64
	err = rows.Scan(&block)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	s.setHighestBlock(big.NewInt(block))

	return nil
}

func (s *SqliteCache) serialize() error {

	snapshotPath := s.Path + "/" + snapshotFileName
	// See if we have a sqlite snapshot we must delete
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		// Nothing to delete
	} else if err == nil {
		// Something to delete
		err = os.Remove(snapshotPath)
		if err != nil {
			return err
		}
	} else {
		return err
	}

	// Now create the db and save to it
	dst, err := sql.Open("sqlite3", "file:"+snapshotPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	return cloneSqlDB(dst, s.db)
}

func (s *SqliteCache) getMinipoolNode(pubkey rptypes.ValidatorPubkey) (common.Address, error) {
	var addr []byte

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return common.Address{}, err
	}
	defer rollback(tx)

	rows, err := tx.Stmt(s.getMinipoolStmt).Query(pubkey[:])
	if err != nil {
		return common.Address{}, err
	}

	if !rows.Next() {
		if err := tx.Commit(); err != nil {
			return common.Address{}, err
		}
		return common.Address{}, &NotFoundError{}
	}

	err = rows.Scan(&addr)
	if err != nil {
		return common.Address{}, err
	}

	if rows.Next() {
		return common.Address{}, fmt.Errorf("retrieved more than one row for a minipool point query")
	}

	return common.BytesToAddress(addr), tx.Commit()
}

func (s *SqliteCache) addMinipoolNode(pubkey rptypes.ValidatorPubkey, nodeAddr common.Address) error {

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, err = tx.Stmt(s.setMinipoolStmt).Exec(pubkey[:], nodeAddr.Bytes())
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SqliteCache) getNodeInfo(nodeAddr common.Address) (*nodeInfo, error) {
	var dbSPStatus int
	var dbFeeDistributor []byte

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer rollback(tx)

	rows, err := tx.Stmt(s.getNodeStmt).Query(nodeAddr.Bytes())
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, &NotFoundError{}
	}

	err = rows.Scan(&dbSPStatus, &dbFeeDistributor)
	if err != nil {
		return nil, err
	}

	if rows.Next() {
		return nil, fmt.Errorf("retrieved more than one row for a minipool point query")
	}

	return &nodeInfo{
		inSmoothingPool: dbSPStatus > 0,
		feeDistributor:  common.BytesToAddress(dbFeeDistributor),
	}, tx.Commit()
}

func (s *SqliteCache) addNodeInfo(nodeAddr common.Address, node *nodeInfo) error {
	var inSP int = 0

	if node.inSmoothingPool {
		inSP = 1
	}

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, err = tx.Stmt(s.setNodeStmt).Exec(nodeAddr.Bytes(), inSP, node.feeDistributor.Bytes())
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SqliteCache) forEachNode(closure ForEachNodeClosure) error {
	var address []byte

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	rows, err := tx.Stmt(s.forEachNodeStmt).Query()
	if err != nil {
		return err
	}

	for rows.Next() {
		err = rows.Scan(&address)
		if err != nil {
			return err
		}

		if !closure(common.BytesToAddress(address)) {
			break
		}
	}

	return tx.Commit()
}

func (s *SqliteCache) addOdaoNode(nodeAddr common.Address) error {

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, err = tx.Stmt(s.addOdaoNodeStmt).Exec(nodeAddr.Bytes())
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SqliteCache) removeOdaoNode(nodeAddr common.Address) error {

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, err = tx.Stmt(s.delOdaoNodeStmt).Exec(nodeAddr.Bytes())
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SqliteCache) forEachOdaoNode(closure ForEachNodeClosure) error {
	var address []byte

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	rows, err := tx.Stmt(s.forEachOdaoNodeStmt).Query()
	if err != nil {
		return err
	}

	for rows.Next() {
		err = rows.Scan(&address)
		if err != nil {
			return err
		}

		if !closure(common.BytesToAddress(address)) {
			break
		}
	}

	return tx.Commit()
}

func (s *SqliteCache) setHighestBlock(block *big.Int) {
	if s.highestBlock.Cmp(block) >= 0 {
		return
	}

	// Someone else owns this pointer, so make a new one
	s.m.Gauge("highest_block").Set(float64(block.Uint64()))
	s.highestBlock = big.NewInt(0)
	s.highestBlock.Add(block, s.highestBlock)
}

func (s *SqliteCache) getHighestBlock() *big.Int {

	return s.highestBlock
}

func (s *SqliteCache) reset() error {
	//Just delete from each of the tables
	_, err := s.db.Exec("DELETE FROM nodes;")
	if err != nil {
		return err
	}

	_, err = s.db.Exec("DELETE FROM minipools;")
	if err != nil {
		return err
	}

	_, err = s.db.Exec("DELETE FROM highest_block;")
	if err != nil {
		return err
	}

	_, err = s.db.Exec("DELETE FROM odao_nodes;")
	if err != nil {
		return err
	}

	s.m.Counter("reset").Inc()
	return nil
}

func (s *SqliteCache) deinit() error {
	// Write the highest block into the db
	block := s.highestBlock.Int64()

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: false, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, err = tx.Stmt(s.setHighestBlockStmt).Exec(block)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	// Save the db to disk
	err = s.serialize()
	if err != nil {
		return err
	}

	s.getMinipoolStmt.Close()
	s.getNodeStmt.Close()
	s.getHighestBlockStmt.Close()
	s.setMinipoolStmt.Close()
	s.setNodeStmt.Close()
	s.setHighestBlockStmt.Close()
	s.forEachNodeStmt.Close()
	s.addOdaoNodeStmt.Close()
	s.delOdaoNodeStmt.Close()
	s.forEachOdaoNodeStmt.Close()
	s.db.Close()
	return nil
}
