from solana.rpc.api import Client
from solders.signature import Signature  # 导入 Signature

# 连接到 Solana 主网
solana_client = Client("https://api.mainnet-beta.solana.com")

# 交易哈希
tx_hash = "326mofUjBahEi71SmddKmPpmLCXjgYUUsQYUyEf18Msd2caU6WGPsxmwx71osjmStBRSBiqQg3F4r2T17zFwGnJe"

# 将字符串转换为 Signature 类型
tx_signature = Signature.from_string(tx_hash)

# 获取交易详情
tx_details = solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)

# 直接打印完整交易数据
print("\n==== 交易详情 (Raw Data) ====\n")
print(tx_details.value)
