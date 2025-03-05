import requests
import json
import csv
import os
import config
import datetime
from tqdm import tqdm  # ✅ 进度条库
from solana.rpc.api import Client
from SolanaSlotFinder import SolanaSlotFinder
from RaydiumPoolFetcher import RaydiumPoolFetcher
from TransactionFetcher import TransactionFetcher
from LogDecoder import LogDecoder
from solders.pubkey import Pubkey
import concurrent.futures
import numpy as np
from tqdm import tqdm

CONFIG = config.CONFIG  # 直接使用 CONFIG


class SolanaFetcher:
    """
    Solana 交易数据抓取器：
    1. 读取 `input.csv` 获取 `mint1, mint2`
    2. 获取 Raydium 流动性池
    3. 读取 `POOL_symbol1_symbol2.csv` 获取 `pool_id`
    4. 调用 `TransactionFetcher.fetch_transactions()` 获取交易签名
    5. 调用 `LogDecoder.decode()` 解析交易详情
    """

    def __init__(self, start_datetime, end_datetime,slot_finder,log_decoder):
        """
        初始化 SolanaFetcher
        :param start_datetime: 起始时间
        :param end_datetime: 结束时间
        """
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.slot_finder = slot_finder
        self.start_timestamp = int(start_datetime.timestamp())
        self.end_timestamp = int(end_datetime.timestamp())
        #self.start_slot = self.slot_finder.find_closest_slot(self.start_timestamp)
        self.start_slot = 323247261
        #self.end_slot = self.slot_finder.find_closest_slot(self.end_timestamp)
        self.end_slot = 323247409
        self.log_decoder = log_decoder

        # 常见稳定币符号
        self.stable_symbols = {"USDC", "USDT", "USDD"}

    def read_input(self):
        """
        读取 `input.csv` 返回所有 `mint1, mint2` 对
        """
        input_file = os.path.join(CONFIG["input_path"], "input.csv")
        if not os.path.exists(input_file):
            print(f"❌ 输入文件未找到: {input_file}")
            return []

        print(f"🔍 读取输入文件: {input_file}")
        with open(input_file, mode="r", newline="") as file:
            reader = csv.reader(file)
            next(reader, None)  # 跳过 CSV 头部
            return [row for row in reader]  # 返回 mint1, mint2 交易对

    @staticmethod
    def print_stage_header(message):
        """
        打印阶段性输出
        """
        border = "=" * 50
        print(f"\n{border}\n <<<<<<<<<< {message}  >>>>>>>>>> \n{border}\n")

    def fetch_pool_by_token(self, mint1, mint2):
        """
        获取 Raydium 流动性池数据
        """
        print(f"📡 获取 {mint1} / {mint2} 的流动性池数据...")
        fetcher = RaydiumPoolFetcher(mint1, mint2)
        fetcher.run()
        return fetcher.mint1symbol, fetcher.mint2symbol

    def read_pool_file(self, symbol1, symbol2):
        """
        读取 `POOL_symbol1_symbol2.csv` 并返回 `pool_id` 列数据
        """
        pool_file = os.path.join(CONFIG["output_path"], "POOL", f"POOL_{symbol1}_{symbol2}.csv")

        if not os.path.exists(pool_file):
            print(f"❌ Pool 文件未找到: {pool_file}")
            return []

        print(f"🔍 读取流动性池文件: {pool_file}")
        pool_ids = []
        with open(pool_file, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if "pool_id" in row:
                    pool_ids.append(row["pool_id"])
        return pool_ids

    def fetch_transactions_for_pool(self, symbol1, symbol2):
        """
        读取 `POOL_symbol1_symbol2.csv` 获取 `pool_id` 并查询交易
        """
        file_name = f"{symbol1}_{symbol2}.csv"
        TX_SIG_fetcher = TransactionFetcher.from_slots(CONFIG["rpc_url1"], self.slot_finder, self.start_slot, self.end_slot, file_name)

        # 读取 `POOL_symbol1_symbol2.csv` 获取 `pool_id`
        market_address_list = self.read_pool_file(symbol1, symbol2)

        # 遍历 `pool_id` 获取交易签名
        for market_address in market_address_list:
            TX_SIG_fetcher.fetch_transactions(market_address)

    def read_signatures_file(self, symbol1, symbol2):
        """
        读取 `SIGNATURE_symbol1_symbol2.csv` 并返回符合 slot 过滤条件的交易签名
        """
        sig_file = os.path.join(CONFIG["output_path"], "SIGNATURE", f"{symbol1}_{symbol2}.csv")

        if not os.path.exists(sig_file):
            print(f"❌ 签名文件未找到: {sig_file}")
            return []

        print(f"🔍 读取交易签名文件: {sig_file}")
        filtered_signatures = []
        with open(sig_file, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                slot = int(row["Slot"])
                if self.start_slot <= slot <= self.end_slot:
                    filtered_signatures.append((row["Signature"], row["Market_Address"]))

        return filtered_signatures

    def process_signature_batch(batch, log_decoder, unstable_symbol):
        """
        处理交易签名批次的函数
        """
        for transaction_signature, market_address in tqdm(batch, desc="Processing Batch", unit="tx", leave=False):
            log_decoder.decode(transaction_signature, market_address, unstable_symbol)

    def run(self):
        """
        运行 SolanaFetcher，处理所有 `mint1, mint2` 交易对
        """
        self.print_stage_header("SOL_FETCHER STARTING")

        # 获取所有交易对
        token_pairs = self.read_input()

        for mint1, mint2 in token_pairs:
            self.print_stage_header("FETCHING POOL")
            symbol1, symbol2 = self.fetch_pool_by_token(mint1, mint2)

            # 识别非稳定币
            unstable_symbol = symbol1 if symbol2 in self.stable_symbols else symbol2
            self.print_stage_header(f"SUCCESS FETCH POOL BY {symbol1} {symbol2}")

            # 获取交易签名
            self.print_stage_header("FETCHING TX")
            self.fetch_transactions_for_pool(symbol1, symbol2)
            self.print_stage_header("FETCH TX SUCCESS")

            # ✅ **添加多线程进度条**
            self.print_stage_header("DECODING TX LOGS")
            tx_signatures = self.read_signatures_file(symbol1, symbol2)

            if not tx_signatures:
                print("⚠️ 没有符合条件的交易签名，跳过解码！")
            else:
                # **划分 tx_signatures 为 10 份**
                num_threads = 10
                signature_batches = np.array_split(tx_signatures, num_threads)

                with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                    futures = [
                        executor.submit(process_signature_batch, batch, self.log_decoder, unstable_symbol)
                        for batch in signature_batches if len(batch) > 0
                    ]

                    # **等待所有线程完成**
                    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures),
                                       desc="Processing Batches"):
                        future.result()

            self.print_stage_header("DECODING TX SUCCESS")


# ========== 主函数 ========== #
if __name__ == "__main__":
    start_datetime = datetime.datetime(2025, 2, 27, 0, 0)
    end_datetime = datetime.datetime(2025, 2, 27, 0, 1)
    slot_finder = SolanaSlotFinder(CONFIG["rpc_url2"])
    log_decoder = LogDecoder(CONFIG["rpc_url3"])

    fetcher = SolanaFetcher(start_datetime, end_datetime,slot_finder,log_decoder)
    fetcher.run()
