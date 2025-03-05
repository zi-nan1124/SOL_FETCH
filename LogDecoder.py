import csv
import json
import os
from solana.rpc.api import Client
from solders.signature import Signature
import config

CONFIG = config.CONFIG  # 直接使用 CONFIG


class LogDecoder:
    def __init__(self, rpc_url, log_enabled=False):
        """
        初始化 Solana RPC 连接
        :param rpc_url: Solana RPC 端点
        :param log_enabled: 是否启用日志（默认 False）
        """
        self.solana_client = Client(rpc_url)
        self.log_enabled = log_enabled  # 控制日志输出

        # 常见稳定币地址映射（Solana 主网）
        self.stablecoins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT"
        }

        print(f"LogDecoder initialized with RPC: {rpc_url}")

    def log(self, message):
        """ 控制日志输出 """
        if self.log_enabled:
            print(message)

    def decode_transaction(self, transaction_signature, market_address):
        """
        解析指定交易的日志，并计算目标账户的代币余额变化，同时返回交易的 blockTime。
        """
        self.log(f"🔍 Decoding transaction: {transaction_signature}")

        # 转换交易签名
        tx_signature = Signature.from_string(transaction_signature)

        # 获取交易数据
        try:
            tx_details = self.solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)
            if tx_details.value is None:
                self.log("⚠️ Transaction not found or is not confirmed yet.")
                return []
        except Exception as e:
            self.log(f"❌ Error fetching transaction: {e}")
            return []

        # 解析 JSON 数据
        tx_details = json.loads(tx_details.value.to_json())

        # 提取 meta 数据
        meta = tx_details.get("meta", {})

        # 获取交易的 blockTime
        block_time = tx_details.get("blockTime", None)

        # 获取交易前后的代币余额
        pre_balances = {
            b["mint"]: float(b["uiTokenAmount"]["uiAmount"])
            for b in meta.get("preTokenBalances", [])
            if b.get("owner") == market_address
        }
        post_balances = {
            b["mint"]: float(b["uiTokenAmount"]["uiAmount"])
            for b in meta.get("postTokenBalances", [])
            if b.get("owner") == market_address
        }

        # 计算余额变化
        balance_changes = []
        for mint in pre_balances.keys() | post_balances.keys():
            pre_amount = pre_balances.get(mint, 0)
            post_amount = post_balances.get(mint, 0)
            change = post_amount - pre_amount
            balance_changes.append({
                "Token": self.stablecoins.get(mint, mint),
                "Mint": mint,
                "Pre Balance": pre_amount,
                "Post Balance": post_amount,
                "Change": change
            })

        return {
            "blockTime": block_time,
            "balanceChanges": balance_changes
        }

    def decode(self, transaction_signature, market_address, non_stable_symbol):
        """
        解析并检查是否是交换事件，并计算非稳定币价格
        """
        # 调用 decode_transaction 并获取 blockTime 和 balanceChanges
        transaction_data = self.decode_transaction(transaction_signature, market_address)

        # 提取 blockTime 和 balanceChanges
        block_time = transaction_data.get("blockTime")
        balance_changes = transaction_data.get("balanceChanges")

        if not balance_changes:
            self.log("没有余额变化")
            return

        # 统计正负 `Change` 数量
        positive_changes = [c for c in balance_changes if c["Change"] > 0]
        negative_changes = [c for c in balance_changes if c["Change"] < 0]

        if len(positive_changes) == 1 and len(negative_changes) == 1:
            self.log(f"✅ 交易 {transaction_signature} 是交换事件")
            self.log(f"账户 {market_address} 代币余额变动如下：")

            stablecoin, non_stablecoin = None, None

            for change in positive_changes + negative_changes:
                if change["Mint"] in self.stablecoins:
                    stablecoin = change
                else:
                    non_stablecoin = change

            if non_stablecoin:
                non_stablecoin["Token"] = non_stable_symbol

            for change in balance_changes:
                self.log(
                    f"- 代币: {change['Token']}, 交易前: {change['Pre Balance']}, 交易后: {change['Post Balance']}, 变动: {change['Change']}")

            # 计算非稳定币价格
            if stablecoin and non_stablecoin:
                stable_amount = abs(stablecoin["Change"])
                non_stable_amount = abs(non_stablecoin["Change"])

                if non_stable_amount > 0:
                    price = stable_amount / non_stable_amount
                    self.log(f"💰 估算的 {non_stablecoin['Token']} 价格: {price} {stablecoin['Token']}")

                    # 传递 blockTime 到 save_to_csv
                    self.save_to_csv(
                        non_stablecoin["Token"],
                        stablecoin["Token"],
                        transaction_signature,
                        non_stable_amount,
                        stable_amount,
                        price,
                        block_time  # 传递交易时间戳
                    )

    def save_to_csv(self, non_stable_symbol, stable_symbol, transaction_signature, non_stable_change, stable_change,
                    price,block_time):
        """
        将交易数据存入 CSV 文件，避免重复写入，并增加 BlockTime 列
        """
        output_folder = os.path.join(CONFIG["output_path"], "DATA")
        os.makedirs(output_folder, exist_ok=True)

        file_name = f"{non_stable_symbol}_{stable_symbol}.csv"
        output_file = os.path.join(output_folder, file_name)

        # 获取交易的 BlockTime
        block_time = block_time

        # 读取已有的交易签名，避免重复写入
        existing_signatures = set()
        if os.path.exists(output_file):
            with open(output_file, mode="r", newline="") as file:
                reader = csv.reader(file)
                next(reader, None)  # 跳过 CSV 头部
                existing_signatures = {row[0] for row in reader}

        if transaction_signature in existing_signatures:
            self.log(f"⚠️ 交易 {transaction_signature} 已存在，跳过写入。")
            return

        # 追加模式写入 CSV
        with open(output_file, mode="a", newline="") as file:
            writer = csv.writer(file)

            # 如果文件为空，则写入表头
            if os.stat(output_file).st_size == 0:
                writer.writerow(["Signature", "Non-Stable Change", "Stable Change", "Price", "BlockTime"])

            writer.writerow([transaction_signature, non_stable_change, stable_change, price, block_time])

        self.log(f"✅ 交易数据已存入 {output_file}，BlockTime: {block_time}")

    def get_block_time(self, transaction_signature):
        """
        获取交易的 BlockTime
        """
        try:
            tx_signature = Signature.from_string(transaction_signature)
            tx_details = self.solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)
            if tx_details.value is None:
                self.log(f"⚠️ 交易 {transaction_signature} 未找到 BlockTime。")
                return "N/A"

            return tx_details.value.block_time  # 返回 BlockTime
        except Exception as e:
            self.log(f"❌ 获取 BlockTime 失败: {e}")
            return "N/A"


# ========== 使用示例 ==========
if __name__ == "__main__":
    rpc_url = "https://wild-boldest-rain.solana-mainnet.quiknode.pro/b95f33839916945a42159c53ceab4d7468a51a69/"

    # 这里 `log_enabled=True` 开启日志输出
    log_decoder = LogDecoder(rpc_url)

    transaction_signature = "2oYXdAh6C8Q21fFby7wZ1jApmpiC69nLDCBTFQbSeVbNPLBdjUTqFQFgmcA2mto4jdeEFJZAKweSNG9MUv93VKrc"
    market_address = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"
    symbol = "SOL"

    log_decoder.decode(transaction_signature, market_address, symbol)
