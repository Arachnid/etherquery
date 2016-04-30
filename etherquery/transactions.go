package etherquery

import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/rpc"
    "github.com/ethereum/go-ethereum/core/state"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/core/vm"
    "google.golang.org/api/bigquery/v2"
)

func logToJsonValue(log *vm.Log) map[string]bigquery.JsonValue {
    return map[string]bigquery.JsonValue {
        "address": log.Address,
        "topics": log.Topics,
        "data": log.Data,
    }
}

func logsToJsonValue(logs []*vm.Log) []map[string]bigquery.JsonValue {
    loginfos := make([]map[string]bigquery.JsonValue, len(logs))
    for i := 0; i < len(logs); i++ {
        loginfos[i] = logToJsonValue(logs[i])
    }
    return loginfos
}

func transactionToJsonValue(block *types.Block, tx *types.Transaction, trace *transactionTrace) *bigquery.TableDataInsertAllRequestRows {
    from, _ := tx.FromFrontier()

    var contractAddress *common.Address = nil
    if trace.receipt.ContractAddress != (common.Address{}) {
        contractAddress = &trace.receipt.ContractAddress
    }

    var errorMsg *string
    if trace.err != nil {
    	msg := trace.err.Error()
    	errorMsg = &msg
    }

    return &bigquery.TableDataInsertAllRequestRows{
        InsertId: tx.Hash().Hex(),
        Json: map[string]bigquery.JsonValue{
            "blockNumber": block.Number().Uint64(),
            "blockHash": block.Hash(),
            "timestamp": block.Time().Uint64(),
            "hash": tx.Hash(),
            "from": from,
            "to": tx.To(),
            "gas": tx.Gas().Uint64(),
            "gasUsed": trace.receipt.GasUsed,
            "gasPrice": tx.GasPrice().Uint64(),
            "input": tx.Data(),
            "logs": logsToJsonValue(trace.receipt.Logs),
            "nonce": tx.Nonce(),
            "value": rpc.NewHexNumber(tx.Value()),
            "contractAddress": contractAddress,
            "error": errorMsg,
        },
    }
}

type transactionExporter struct {
	writer *batchedBigqueryWriter
}

func (self *transactionExporter) setWriter(writer *batchedBigqueryWriter) { self.writer = writer }
func (self *transactionExporter) getTableName() string { return "transactions" }
func (self *transactionExporter) exportGenesis(block *types.Block, world state.World) {}

func (self *transactionExporter) export(data *blockData) {
    for i, tx := range data.block.Transactions() {
        self.writer.add(transactionToJsonValue(data.block, tx, data.trace.transactions[i]))
    }
}
