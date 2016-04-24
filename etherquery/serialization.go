package etherquery

import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core"
    "github.com/ethereum/go-ethereum/rpc"
    "github.com/ethereum/go-ethereum/core/types"
)

type TransactionInfo struct {
    From            common.Address  `json:"from"`
    Gas             uint64          `json:"gas"`
    GasPrice        uint64          `json:"gasPrice"`
    Hash            common.Hash     `json:"hash"`
    Input           []byte          `json:"input"`
    Nonce           uint64          `json:"nonce"`
    To              *common.Address `json:"to"`
    Value           *rpc.HexNumber  `json:"value"`
}

type BlockInfo struct {
    Number          uint64          `json:"number"`
    Hash            common.Hash     `json:"hash"`
    ParentHash      common.Hash     `json:"parentHash"`
    Nonce           *rpc.HexNumber  `json:"nonce"`
    Miner           common.Address  `json:"miner"`
    Difficulty      *rpc.HexNumber  `json:"difficulty"`
    TotalDifficulty *rpc.HexNumber  `json:"totalDifficulty"`
    ExtraData       []byte          `json:"extraData"`
    Size            int64           `json:"size"`
    GasLimit        uint64          `json:"gasLimit"`
    GasUsed         uint64          `json:"gasUsed"`
    Timestamp       uint64          `json:"timestamp"`
    Transactions    []*TransactionInfo `json:"transactions"`
}

func getTransactionInfo(tx *types.Transaction) *TransactionInfo {
    from, _ := tx.FromFrontier()

    return &TransactionInfo {
        From:           from,
        Gas:            tx.Gas().Uint64(),
        GasPrice:       tx.GasPrice().Uint64(),
        Hash:           tx.Hash(),
        Input:          tx.Data(),
        Nonce:          tx.Nonce(),
        To:             tx.To(),
        Value:          rpc.NewHexNumber(tx.Value()),
    }
}

func getTransactionInfos(txs []*types.Transaction) []*TransactionInfo {
    txinfos := make([]*TransactionInfo, len(txs))
    for i := 0; i < len(txinfos); i++ {
        txinfos[i] = getTransactionInfo(txs[i])
    }
    return txinfos
}

func getBlockInfo(bc *core.BlockChain, b *types.Block) *BlockInfo {
    return &BlockInfo{
        Number:         b.Number().Uint64(),
        Hash:           b.Hash(),
        ParentHash:     b.ParentHash(),
        Nonce:          rpc.NewHexNumber(b.Header().Nonce.Uint64()),
        Miner:          b.Coinbase(),
        Difficulty:     rpc.NewHexNumber(b.Difficulty()),
        TotalDifficulty:rpc.NewHexNumber(bc.GetTd(b.Hash())),
        ExtraData:      b.Extra(),
        Size:           b.Size().Int64(),
        GasLimit:       b.GasLimit().Uint64(),
        GasUsed:        b.GasLimit().Uint64(),
        Timestamp:      b.Time().Uint64(),
        Transactions:   getTransactionInfos(b.Transactions()),
    }
}

func getBlockInfos(bc *core.BlockChain, blocks []*types.Block) []*BlockInfo {
    blkinfos := make([]*BlockInfo, len(blocks))
    for i := 0; i < len(blkinfos); i++ {
        blkinfos[i] = getBlockInfo(bc, blocks[i]);
    }
    return blkinfos
}
