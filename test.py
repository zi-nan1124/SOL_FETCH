import csv
import json
import os
from solana.rpc.api import Client
from solders.signature import Signature
import config
import time
from functools import wraps

CONFIG = config.CONFIG  # 直接使用 CONFIG


def measure_time(func):
    """ 装饰器：测量函数执行时间 """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} 执行时间: {elapsed_time:.6f} 秒")
        return result

    return wrapper


class LogDecoder:
    def __init__(self, rpc_url, log_enabled=True):
        self.solana_client = Client(rpc_url)
        self.log_enabled = log_enabled
        self.coins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
            "So11111111111111111111111111111111111111112": "WSOL"
        }
        self.total_sleep_time = 0
        self.total_attempts = 0
        self.successful_attempts = 0
        print(f"LogDecoder 已初始化，使用 RPC: {rpc_url}")

    def log(self, message):
        if self.log_enabled:
            print(message)

    @measure_time
    def get_transaction_with_retries(self, tx_signature, max_retries=5, wait_time=1):
        total_wait_time = 0
        for attempt in range(1, max_retries + 1):
            self.total_attempts += 1
            start_request_time = time.perf_counter()
            try:
                tx_details = self.solana_client.get_transaction(tx_signature, max_supported_transaction_version=0)
                response_time = time.perf_counter() - start_request_time
                self.log(f"第 {attempt} 次请求响应时间: {response_time:.6f} 秒")

                if tx_details.value is None:
                    self.log("交易未找到或尚未确认。")
                    return None

                self.successful_attempts += 1
                self.log(f"交易 {tx_signature} 在第 {attempt} 次尝试中成功获取")
                return tx_details
            except Exception as e:
                response_time = time.perf_counter() - start_request_time
                self.log(f"获取交易失败 (第 {attempt}/{max_retries} 次尝试): {e}")
                self.log(f"第 {attempt} 次请求响应时间: {response_time:.6f} 秒")

                if attempt < max_retries:
                    self.log(f"将在 {wait_time} 秒后重试...")
                    total_wait_time += wait_time
                    self.total_sleep_time += wait_time
                    time.sleep(wait_time)
                else:
                    self.log(f"所有 {max_retries} 次尝试均失败，跳过交易 {tx_signature}。")
                    self.log(f"总休眠时间: {self.total_sleep_time} 秒")
                    return None
    def decode_transaction(self, transaction_signature, market_address):
        """
        解析指定交易的日志，并计算目标账户的代币余额变化，同时返回交易的 blockTime。
        """
        self.log(f"🔍 Decoding transaction: {transaction_signature}")

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
        start_time = time.perf_counter()
        transaction_data = self.decode_transaction(transaction_signature, market_address)
        end_time = time.perf_counter()
        self.log(f"程序总执行时间: {end_time - start_time:.6f} 秒")

        if self.total_attempts > 0:
            success_rate = (self.successful_attempts / self.total_attempts) * 100
            self.log(f"休眠后连接成功率: {success_rate:.2f}%")


# ========== 使用示例 ==========
if __name__ == "__main__":
    import time

    rpc_url = "https://wild-boldest-rain.solana-mainnet.quiknode.pro/b95f33839916945a42159c53ceab4d7468a51a69/"

    # 这里 `log_enabled=True` 开启日志输出
    log_decoder = LogDecoder(rpc_url)

    transaction_signature = "3XZp6PAJT9e2k2t5U1mdo2kc9boDG69JjeV5oUwquNG3SLJigQMDHoYhb7TrZUsHCSyMDyV4r4QSH6ynuw17Jj89"
    market_address = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"

    start_time = time.perf_counter()

    for i in range(10):
        print(f"🔄 Running iteration {i+1}/100")
        log_decoder.decode(transaction_signature, market_address)

    end_time = time.perf_counter()
    print(f"✅ Total execution time for 100 iterations: {end_time - start_time:.6f} seconds")

