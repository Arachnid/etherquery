package etherquery

import (
    "bytes"
    "encoding/json"
    "log"
    "time"

    "github.com/ethereum/go-ethereum/core"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/eth"
    "github.com/ethereum/go-ethereum/event"
    "github.com/ethereum/go-ethereum/node"
    "github.com/ethereum/go-ethereum/p2p"
    "github.com/ethereum/go-ethereum/rpc"
    "golang.org/x/net/context"
    "golang.org/x/oauth2/google"
    "google.golang.org/api/bigquery/v2"
)

type EtherQueryConfig struct {
    Project     string
    Dataset     string
    Table       string
}

type EtherQuery struct {
    config      *EtherQueryConfig
    ctx         *node.ServiceContext
    ethereum    *eth.Ethereum
    server      *p2p.Server
    headSub     event.Subscription
    bqService   *bigquery.Service
}

func New(config *EtherQueryConfig, ctx *node.ServiceContext) (node.Service, error) {
    log.Print("Creating etherquery service.")

    var ethereum *eth.Ethereum
    if err := ctx.Service(&ethereum); err != nil {
        return nil, err
    }

    return &EtherQuery {
        config: config,
        ethereum: ethereum,
        ctx: ctx,
        server: nil,
    }, nil
}

func (eq *EtherQuery) Protocols() ([]p2p.Protocol) {
    return []p2p.Protocol{}
}

func (eq *EtherQuery) APIs() ([]rpc.API) {
    return []rpc.API{}
}

func batchBlocks(ch <-chan *types.Block, d time.Duration, items int) (<- chan []*types.Block) {
    batches := make(chan []*types.Block)
    go func() {
        more := true
        for more {
            batch := make([]*types.Block, 1)
            if batch[0], more = <-ch; !more {
                return
            }
            
            timeout := time.After(d)
            batching: for {
                var block *types.Block;
                select {
                case block, more = <-ch:
                    batch = append(batch, block)
                    if len(batch) >= items {
                        break batching
                    }
                case <-timeout:
                    break batching
                }
            }
            batches <- batch;
        }
    }()

    return batches
}

func (eq *EtherQuery) serializeBatch(batch []*types.Block) ([]byte, error) {
    info := getBlockInfos(eq.ethereum.BlockChain(), batch)

    var buf bytes.Buffer
    encoder := json.NewEncoder(&buf) 
    for i := 0; i < len(info); i++ {
        if err := encoder.Encode(info[i]); err != nil {
            return nil, err
        }
    }

    return buf.Bytes(), nil
}

func (eq *EtherQuery) consumeBlocks(ch <-chan []*types.Block) {
    for batch := range ch {
        log.Printf("Received batch of %v records starting with %v.", len(batch), batch[0].Number().Uint64())
        data, err := eq.serializeBatch(batch)
        if err != nil {
            log.Printf("Error loading batch starting with %v: %v", batch[0].Number().Uint64(), err)
            continue
        }
        go uploadData(eq.bqService, eq.config.Project, eq.config.Dataset, eq.config.Table, len(batch), data)
    }
}

func (eq *EtherQuery) watchHead(ch <-chan *event.Event) {
    chain := eq.ethereum.BlockChain()
    lastSeen := chain.CurrentBlock().Number().Uint64()

    blocks := make(chan *types.Block)
    defer close(blocks)
    go eq.consumeBlocks(batchBlocks(blocks, time.Second * 10, 500))

    for e := range ch {
        switch data := e.Data.(type) {
        case core.ChainHeadEvent:
            newNumber := data.Block.Number().Uint64()
            for ; lastSeen <= newNumber; lastSeen++ {
                blocks <- chain.GetBlockByNumber(lastSeen)
            }
        default:
            log.Printf("Expected ChainHeadEvent, got %T", data)
        }
    }
}

func (eq *EtherQuery) Start(server *p2p.Server) error {
    log.Print("Starting etherquery service.")

    eq.server = server
    eq.headSub = eq.ctx.EventMux.Subscribe(core.ChainHeadEvent{})

    client, err := google.DefaultClient(context.Background(), bigquery.BigqueryInsertdataScope)
    if err != nil {
        return err
    }
    bqService, err := bigquery.New(client)
    if err != nil {
        return err
    }
    eq.bqService = bqService

    go eq.watchHead(eq.headSub.Chan())
    return nil
}

func (eq *EtherQuery) Stop() error {
    log.Print("Stopping etherquery service.")
    eq.headSub.Unsubscribe()
    return nil
}
