import json
from solana.rpc.api import Client
from solders.signature import Signature

# 连接 Solana RPC
solana_client = Client("https://api.mainnet-beta.solana.com")

# 交易签名
tx_hash = "326mofUjBahEi71SmddKmPpmLCXjgYUUsQYUyEf18Msd2caU6WGPsxmwx71osjmStBRSBiqQg3F4r2T17zFwGnJe"
tx_signature = Signature.from_string(tx_hash)

# 获取交易数据
tx_details = solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)

# 转换 JSON 结构
tx_details = json.loads(tx_details.value.to_json())

# 目标账户地址
market_address = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"

# 提取 meta 数据
meta = tx_details.get("meta", {})

# 获取交易前后的代币余额
pre_balances = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"]) for b in meta.get("preTokenBalances", []) if b.get("owner") == market_address}
post_balances = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"]) for b in meta.get("postTokenBalances", []) if b.get("owner") == market_address}

# 计算余额变化
balance_changes = []
for mint in pre_balances.keys() | post_balances.keys():  # 取并集，确保所有代币都被处理
    pre_amount = pre_balances.get(mint, 0)  # 如果之前没有这个代币，视为 0
    post_amount = post_balances.get(mint, 0)  # 如果之后没有这个代币，视为 0
    change = post_amount - pre_amount
    balance_changes.append({
        "Token": mint,
        "Pre Balance": pre_amount,
        "Post Balance": post_amount,
        "Change": change
    })

# 输出结果
if balance_changes:
    print(f"账户 {market_address} 的代币余额变动如下：")
    for change in balance_changes:
        print(f"- 代币: {change['Token']}, 交易前: {change['Pre Balance']}, 交易后: {change['Post Balance']}, 变动: {change['Change']}")
else:
    print(f"账户 {market_address} 在该交易中没有代币余额变动。")
