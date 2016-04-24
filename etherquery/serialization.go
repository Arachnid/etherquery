package etherquery

import (
    "github.com/ethereum/go-ethereum/core"
    "github.com/ethereum/go-ethereum/rpc"
    "github.com/ethereum/go-ethereum/core/types"
    "google.golang.org/api/bigquery/v2"
)

func transactionToJsonValue(tx *types.Transaction) map[string]bigquery.JsonValue {
    from, _ := tx.FromFrontier()

    return map[string]bigquery.JsonValue{
        "from": from,
        "gas": tx.Gas().Uint64(),
        "gasPrice": tx.GasPrice().Uint64(),
        "hash": tx.Hash(),
        "input": tx.Data(),
        "nonce": tx.Nonce(),
        "to": tx.To(),
        "value": rpc.NewHexNumber(tx.Value()),
    }
}

func transactionsToJsonValue(txs []*types.Transaction) []map[string]bigquery.JsonValue {
    txinfos := make([]map[string]bigquery.JsonValue, len(txs))
    for i := 0; i < len(txinfos); i++ {
        txinfos[i] = transactionToJsonValue(txs[i])
    }
    return txinfos
}

func blockToJsonValue(bc *core.BlockChain, b *types.Block) *bigquery.TableDataInsertAllRequestRows {
    return &bigquery.TableDataInsertAllRequestRows{
        InsertId: b.Hash().Hex(),
        Json: map[string]bigquery.JsonValue{
            "number": b.Number().Uint64(),
            "hash": b.Hash(),
            "parentHash": b.ParentHash(),
            "nonce": rpc.NewHexNumber(b.Header().Nonce.Uint64()),
            "miner": b.Coinbase(),
            "difficulty": rpc.NewHexNumber(b.Difficulty()),
            "totalDifficulty": rpc.NewHexNumber(bc.GetTd(b.Hash())),
            "extraData": b.Extra(),
            "size": b.Size().Int64(),
            "gasLimit": b.GasLimit().Uint64(),
            "gasUsed": b.GasLimit().Uint64(),
            "timestamp": b.Time().Uint64(),
            "transactions": transactionsToJsonValue(b.Transactions()),
        },
    }
}

func blocksToJsonValue(bc *core.BlockChain, blocks []*types.Block) []*bigquery.TableDataInsertAllRequestRows {
    values := make([]*bigquery.TableDataInsertAllRequestRows, len(blocks))
    for i := 0; i < len(blocks); i++ {
        values[i] = blockToJsonValue(bc, blocks[i])
    }
    return values
}
