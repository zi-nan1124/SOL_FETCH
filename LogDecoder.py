import csv
import json
import os
from solana.rpc.api import Client
from solders.signature import Signature
import config
import time
import threading

CONFIG = config.CONFIG  # 直接使用 CONFIG


class LogDecoder:
    _global_lock = threading.Lock()  # 共享锁

    def __init__(self, rpc_url, log_enabled=True):
        """
        初始化 Solana RPC 连接
        :param rpc_url: Solana RPC 端点
        :param log_enabled: 是否启用日志（默认 False）
        """
        self.solana_client = Client(rpc_url)
        self.log_enabled = log_enabled  # 控制日志输出

        # 常见稳定币地址映射（Solana 主网）
        self.coins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
            "So11111111111111111111111111111111111111112": "WSOL"

        }
        print(f"LogDecoder initialized with RPC: {rpc_url}")

    def log(self, message):
        """ 控制日志输出 """
        if self.log_enabled:
            print(message)


    def get_transaction_with_retries(self, tx_signature, max_retries=100, wait_time=1):
        """
        带重试机制的 Solana 交易查询
        :param tx_signature: 交易签名
        :param max_retries: 最大重试次数
        :param wait_time: 每次重试的等待时间（秒）
        :return: 交易详情（dict） 或 None（查询失败）
        """
        for attempt in range(1, max_retries + 1):
            try:
                tx_details = self.solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)

                # 如果交易未找到
                if tx_details.value is None:
                    self.log("⚠️ Transaction not found or is not confirmed yet.")
                    return None  # 确保返回 None 而不是 []

                # 成功返回交易详情
                self.log(f"✅ Transaction {tx_signature} fetched successfully on attempt {attempt}")
                return tx_details

            except Exception as e:
                self.log(f"❌ Error fetching transaction (attempt {attempt}/{max_retries}): {e}")

                if attempt < max_retries:
                    self.log(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.log(f"🚨 All {max_retries} attempts failed. Skipping transaction {tx_signature}.")
                    return None  # 所有重试都失败，返回 None

    def decode_transaction(self, transaction_signature, market_address):
        """
        解析指定交易的日志，并计算目标账户的代币余额变化，同时返回交易的 blockTime。
        """
        #self.log(f"🔍 Decoding transaction: {transaction_signature}")

        # 转换交易签名
        tx_signature = Signature.from_string(transaction_signature)

        # **使用带重试机制的 get_transaction**
        tx_details = self.get_transaction_with_retries(tx_signature)

        if tx_details is None:
            print("\nerror! get_transaction_with_retries failed.")
            self.log(f"⚠️ Skipping transaction {transaction_signature} due to repeated failures.")
            return {"blockTime": None, "balanceChanges": []}  # 返回空结果

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
                "Token": self.coins.get(mint, mint),
                "Mint": mint,
                "Pre Balance": pre_amount,
                "Post Balance": post_amount,
                "Change": change
            })

        return {
            "blockTime": block_time,
            "balanceChanges": balance_changes
        }

    def decode(self, transaction_signature, market_address):
        """
        解析交易日志，并直接记录两个代币的 Change 和 Symbol
        """
        # 获取交易数据
        transaction_data = self.decode_transaction(transaction_signature, market_address)

        # 提取 blockTime 和 balanceChanges
        block_time = transaction_data.get("blockTime")
        balance_changes = transaction_data.get("balanceChanges")

        if not balance_changes:
            self.log("没有余额变化")
            return

        # 记录变动的代币
        if len(balance_changes) == 2:  # 仅当有两个代币变动时
            token1, token2 = balance_changes

            #self.log(f"✅ 交易 {transaction_signature} 是交换事件")
            #self.log(f"账户 {market_address} 代币余额变动如下：")

            for change in balance_changes:
                self.log(
                    f"- 代币: {change['Token']}, 交易前: {change['Pre Balance']}, 交易后: {change['Post Balance']}, 变动: {change['Change']}")

            # 存储数据到 CSV
            # 存储数据到 CSV，使用线程锁保护
            try:
                with LogDecoder._global_lock:
                    self.save_to_csv(
                        token1["Token"], token2["Token"], transaction_signature,
                        abs(token1["Change"]), abs(token2["Change"]), block_time
                    )
            except Exception as e:
                print(f"❌ 写入 CSV 失败: {e}")


    def save_to_csv(self, token1_symbol, token2_symbol, transaction_signature, token1_change, token2_change,
                    block_time):
        """
        将交易数据存入 CSV 文件，不区分稳定币，直接记录两种代币的 Change 和 Symbol
        """
        output_folder = os.path.join(CONFIG["output_path"], "DATA")
        os.makedirs(output_folder, exist_ok=True)

        file_name = f"{token1_symbol}_{token2_symbol}.csv"
        output_file = os.path.join(output_folder, file_name)

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
                writer.writerow(["Signature", "Token1", "Token1_Change", "Token2", "Token2_Change", "BlockTime"])

            writer.writerow(
                [transaction_signature, token1_symbol, token1_change, token2_symbol, token2_change, block_time])

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

    transaction_signature = "3XZp6PAJT9e2k2t5U1mdo2kc9boDG69JjeV5oUwquNG3SLJigQMDHoYhb7TrZUsHCSyMDyV4r4QSH6ynuw17Jj89"
    market_address = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"

    log_decoder.decode(transaction_signature, market_address)
