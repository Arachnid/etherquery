package etherquery

import (
    "github.com/ethereum/go-ethereum/core/state"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/rpc"
    "google.golang.org/api/bigquery/v2"
)

func blockToJsonValue(d *blockData) *bigquery.TableDataInsertAllRequestRows {
    return &bigquery.TableDataInsertAllRequestRows{
        InsertId: d.block.Hash().Hex(),
        Json: map[string]bigquery.JsonValue{
            "number": d.block.Number().Uint64(),
            "hash": d.block.Hash(),
            "parentHash": d.block.ParentHash(),
            "nonce": rpc.NewHexNumber(d.block.Header().Nonce.Uint64()),
            "miner": d.block.Coinbase(),
            "difficulty": rpc.NewHexNumber(d.block.Difficulty()),
            "totalDifficulty": rpc.NewHexNumber(d.totalDifficulty),
            "extraData": d.block.Extra(),
            "size": d.block.Size().Int64(),
            "gasLimit": d.block.GasLimit().Uint64(),
            "gasUsed": d.block.GasLimit().Uint64(),
            "timestamp": d.block.Time().Uint64(),
            "transactionCount": len(d.block.Transactions()),
        },
    }
}

type blockExporter struct {
	writer *batchedBigqueryWriter
}

func (self *blockExporter) setWriter(writer *batchedBigqueryWriter) { self.writer = writer }
func (self *blockExporter) getTableName() string { return "blocks" }
func (self *blockExporter) exportGenesis(block *types.Block, world state.World) {}

func (self *blockExporter) export(data *blockData) {
    self.writer.add(blockToJsonValue(data))
}
