package etherquery

import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/rpc"
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

func transactionToJsonValue(block *types.Block, tx *types.Transaction, receipt *types.Receipt) *bigquery.TableDataInsertAllRequestRows {
    from, _ := tx.FromFrontier()

    var contractAddress *common.Address = nil
    if receipt.ContractAddress != (common.Address{}) {
        contractAddress = &receipt.ContractAddress
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
            "gasUsed": receipt.GasUsed,
            "gasPrice": tx.GasPrice().Uint64(),
            "input": tx.Data(),
            "logs": logsToJsonValue(receipt.Logs),
            "nonce": tx.Nonce(),
            "value": rpc.NewHexNumber(tx.Value()),
            "contractAddress": contractAddress,
        },
    }
}

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
