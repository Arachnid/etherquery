package etherquery

import (
    "log"
    "math/big"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/state"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/rpc"
    "google.golang.org/api/bigquery/v2"
)

func transferToJsonValue(block *types.Block, idx int, transfer *valueTransfer) *bigquery.TableDataInsertAllRequestRows {
    insertId := big.NewInt(int64(idx))
    insertId.Add(insertId, block.Hash().Big())

    var transactionHash *common.Hash
    if transfer.transactionHash != (common.Hash{}) {
        transactionHash = &transfer.transactionHash
    }

    return &bigquery.TableDataInsertAllRequestRows{
        InsertId: insertId.Text(16),
        Json: map[string]bigquery.JsonValue{
            "blockNumber": block.Number().Uint64(),
            "blockHash": block.Hash(),
            "timestamp": block.Time().Uint64(),
            "transactionHash": transactionHash,
            "transferIndex": idx,
            "depth": transfer.depth,
            "from": transfer.src,
            "to": transfer.dest,
            "fromBalance": rpc.NewHexNumber(transfer.srcBalance),
            "toBalance": rpc.NewHexNumber(transfer.destBalance),
            "value": rpc.NewHexNumber(transfer.value),
            "type": transfer.kind,
        },
    }
}

type transferExporter struct {
    writer *batchedBigqueryWriter
}

func (self *transferExporter) setWriter(writer *batchedBigqueryWriter) { self.writer = writer }
func (self *transferExporter) getTableName() string { return "transfers" }

func (self *transferExporter) export(data *blockData) {
    for i, transfer := range data.trace.transfers {
        self.writer.add(transferToJsonValue(data.block, i, transfer))
    }
}

func (self *transferExporter) exportGenesis(block *types.Block, world state.World) {
    i := 0
    for address, account := range world.Accounts {
        balance, ok := new(big.Int).SetString(account.Balance, 10)
        if !ok {
            log.Panicf("Could not decode balance of genesis account")
        }
        transfer := &valueTransfer{
            depth: 0,
            transactionHash: common.Hash{},
            src: common.Address{},
            srcBalance: big.NewInt(0),
            dest: common.HexToAddress(address),
            destBalance: balance,
            value: balance,
            kind: "GENESIS",
        }
        self.writer.add(transferToJsonValue(block, i, transfer))
        i += 1
    }
}
