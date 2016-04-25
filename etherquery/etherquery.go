package etherquery

import (
    "bytes"
    "encoding/binary"
    "log"
    "math/big"
    "time"

    "github.com/ethereum/go-ethereum/core"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/eth"
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/event"
    "github.com/ethereum/go-ethereum/node"
    "github.com/ethereum/go-ethereum/p2p"
    "github.com/ethereum/go-ethereum/rpc"
    "golang.org/x/net/context"
    "golang.org/x/oauth2/google"
    "google.golang.org/api/bigquery/v2"
)

const DATA_VERSION uint64 = 1

type EtherQueryConfig struct {
    Project         string
    Dataset         string
    BatchInterval   time.Duration
    BatchSize       int
}

type EtherQuery struct {
    bqService   *bigquery.Service
    config      *EtherQueryConfig
    db          ethdb.Database
    ethereum    *eth.Ethereum
    headSub     event.Subscription
    mux         *event.TypeMux
    server      *p2p.Server
}

func New(config *EtherQueryConfig, ctx *node.ServiceContext) (node.Service, error) {
    var ethereum *eth.Ethereum
    if err := ctx.Service(&ethereum); err != nil {
        return nil, err
    }

    db, err := ctx.OpenDatabase("etherquery", 16, 16)
    if err != nil {
        return nil, err
    }

    return &EtherQuery {
        bqService:      nil,
        config:         config,
        db:             db,
        ethereum:       ethereum,
        headSub:        nil,
        mux:            ctx.EventMux,
        server:         nil,
    }, nil
}

func (eq *EtherQuery) Protocols() ([]p2p.Protocol) {
    return []p2p.Protocol{}
}

func (eq *EtherQuery) APIs() ([]rpc.API) {
    return []rpc.API{}
}

type blockData struct {
    block           *types.Block
    receipts        []*types.Receipt
    totalDifficulty *big.Int
}

type exporter func(*batchedBigqueryWriter, *blockData)

func blockExporter(writer *batchedBigqueryWriter, data *blockData) {
    writer.add(blockToJsonValue(data))
}

func transactionExporter(writer *batchedBigqueryWriter, data *blockData) {
    for i, tx := range data.block.Transactions() {
        writer.add(transactionToJsonValue(data.block, tx, data.receipts[i]))
    }
}

type exporterConfig struct {
    function    exporter
    tableName   string
}

var EXPORTERS []exporterConfig = []exporterConfig{
    exporterConfig{blockExporter, "blocks"},
    exporterConfig{transactionExporter, "transactions"},
}

func (eq *EtherQuery) processBlocks(ch <-chan *types.Block) {
    writers := make([]*batchedBigqueryWriter, len(EXPORTERS))
    for i, conf := range EXPORTERS {
        writers[i] = newBatchedBigqueryWriter(eq.bqService, eq.config.Project, eq.config.Dataset, conf.tableName, eq.config.BatchInterval, eq.config.BatchSize)
        writers[i].start()
    }

    chainDb := eq.ethereum.ChainDb()

    for block := range ch {
        if block.Number().Uint64() % 1000 == 0 {
            log.Printf("Processing block %v...", block.Number().Uint64());
        }

        blockData := &blockData{
            block: block,
            receipts: core.GetBlockReceipts(chainDb, block.Hash()),
            totalDifficulty: eq.ethereum.BlockChain().GetTd(block.Hash()),
        }

        for i, conf := range EXPORTERS {
            conf.function(writers[i], blockData)
        }
        eq.putLastBlock(block.Number().Uint64())
    }
}

func (eq *EtherQuery) getInt(key string) (uint64, error) {
    data, err := eq.db.Get([]byte(key))
    if err != nil {
        return 0, err
    }

    var value uint64
    err = binary.Read(bytes.NewReader(data), binary.LittleEndian, &value)
    if err != nil {
        return 0, err
    }

    return value, nil
}

func (eq *EtherQuery) putInt(key string, value uint64) error {
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, value)
    if err != nil {
        return err
    }
    return eq.db.Put([]byte(key), buf.Bytes())
}

func (eq *EtherQuery) getLastBlock() uint64 {
    dataVersion, err := eq.getInt("dataVersion")
    if err != nil || dataVersion < DATA_VERSION {
        log.Printf("Obsolete dataVersion")
        eq.putInt("dataVersion", DATA_VERSION)
        eq.putInt("lastBlock", 0)
        return 0
    }
    lastBlock, err := eq.getInt("lastBlock")
    if err != nil {
        return 0
    }
    return lastBlock
}

func (eq *EtherQuery) putLastBlock(block uint64) {
    eq.putInt("lastBlock", block)
}

func (eq *EtherQuery) consumeBlocks() {
    blocks := make(chan *types.Block, 256)
    go eq.processBlocks(blocks)
    defer close(blocks)

    chain := eq.ethereum.BlockChain()
    lastBlock := eq.getLastBlock()

    // First catch up
    for lastBlock < chain.CurrentBlock().Number().Uint64() {
        blocks <- chain.GetBlockByNumber(lastBlock)
        lastBlock += 1
    }

    // Now, subscribe to new blocks as they arrive
    ch := eq.mux.Subscribe(core.ChainHeadEvent{}).Chan()
    for e := range ch {
        if event, ok := e.Data.(core.ChainHeadEvent); ok {
            newBlock := event.Block.Number().Uint64()
            for ; lastBlock <= newBlock; lastBlock++ {
                blocks <- chain.GetBlockByNumber(lastBlock)
            }
        } else {
            log.Printf("Expected ChainHeadEvent, got %T", e.Data)
        }
    }
}

func (eq *EtherQuery) Start(server *p2p.Server) error {
    log.Print("Starting etherquery service.")

    eq.server = server

    client, err := google.DefaultClient(context.Background(), bigquery.BigqueryInsertdataScope)
    if err != nil {
        return err
    }
    bqService, err := bigquery.New(client)
    if err != nil {
        return err
    }
    eq.bqService = bqService

    go eq.consumeBlocks()
    return nil
}

func (eq *EtherQuery) Stop() error {
    log.Print("Stopping etherquery service.")
    eq.headSub.Unsubscribe()
    return nil
}
