import os
import csv
import pandas as pd
import config
import datetime
from tqdm import tqdm  # ✅ 进度条库
from SolanaSlotFinder import SolanaSlotFinder
from RaydiumPoolFetcher import RaydiumPoolFetcher
from TransactionFetcher import TransactionFetcher
from LogDecoder import LogDecoder
import concurrent.futures
import threading
from solders.pubkey import Pubkey
import time
import logging
import httpx
from solana.exceptions import SolanaRpcException


CONFIG = config.CONFIG  # 直接使用 CONFIG


class SolanaFetcher:
    """
    Solana 交易数据抓取器：支持两种初始化方式：
    1. 传入 `start_datetime` 和 `end_datetime`，自动计算 Slot
    2. 传入 `start_slot` 和 `end_slot`，直接使用指定 Slot
    """

    def __init__(self, start_slot, end_slot, rpc_url):
        """
        初始化 SolanaFetcher（使用 Slot 直接初始化）
        :param start_slot: 起始 Slot
        :param end_slot: 结束 Slot
        :param slot_finder: Slot 解析器
        :param log_decoder: 交易日志解码器
        """
        self.start_slot = start_slot
        self.end_slot = end_slot
        self.rpc_url = rpc_url
        self.slot_finder = SolanaSlotFinder(rpc_url)
        self.TX_SIG_fetcher = TransactionFetcher.from_slots(self.rpc_url, self.slot_finder, self.start_slot, self.end_slot)

        # **从 CONFIG 读取多个 RPC URL**
        self.rpc_urls = [config.CONFIG[key] for key in config.CONFIG if key.startswith("rpc_url")]

        if not self.rpc_urls:
            raise ValueError("❌ 没有可用的 RPC 端点，请检查 CONFIG 配置！")

        # **创建多个 LogDecoder 实例**
        self.log_decoders = [LogDecoder(url) for url in self.rpc_urls]

        print(f"✅ 初始化 {len(self.log_decoders)} 个 LogDecoder 实例，均衡负载 Solana 节点")

        # 常见稳定币符号
        self.stable_symbols = {"USDC", "USDT", "USDD"}

    @classmethod
    def from_datetime(cls, start_datetime, end_datetime,rpc_url):
        """
        使用时间戳初始化 SolanaFetcher（自动计算 Slot）
        :param start_datetime: 起始时间（datetime 对象）
        :param end_datetime: 结束时间（datetime 对象）
        :param slot_finder: Slot 解析器
        :param log_decoder: 交易日志解码器
        :return: SolanaFetcher 实例
        """
        start_timestamp = int(start_datetime.timestamp())
        end_timestamp = int(end_datetime.timestamp())
        slot_finder = SolanaSlotFinder(rpc_url)
        start_slot = slot_finder.find_closest_slot(start_timestamp)
        end_slot = slot_finder.find_closest_slot(end_timestamp)

        return cls(start_slot, end_slot, rpc_url)


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
        读取 `POOL_symbol1_symbol2.csv` 获取 `pool_id` 并使用多线程查询交易
        """
        file_name = f"{symbol1}_{symbol2}.csv"

        # 读取 `POOL_symbol1_symbol2.csv` 获取 `pool_id`
        market_address_list = self.read_pool_file(symbol1, symbol2)

        # 线程数设为 `15`
        max_threads = 15

        def fetch_for_market(market_address):
            """单独处理一个市场地址的交易获取"""
            attempt = 0
            max_retries = 5  # 最大重试次数

            while attempt < max_retries:
                try:
                    logging.info(
                        f"Fetching transactions for Market Address: {market_address} (Attempt {attempt + 1}/{max_retries})")

                    # 发送请求
                    self.TX_SIG_fetcher.fetch_transactions(market_address,file_name)

                    # 成功获取数据，跳出重试循环
                    break

                except (httpx.ReadTimeout, SolanaRpcException) as e:
                    logging.warning(f"Request failed: {e}. Retrying... (Attempt {attempt + 1})")

                    # 计算指数退避时间：2^attempt 秒（最大 32 秒）
                    wait_time = min(2 ** attempt, 32)
                    time.sleep(wait_time)

                    attempt += 1

            if attempt == max_retries:
                logging.error(
                    f"Failed to fetch transactions for {market_address} after {max_retries} attempts. Skipping...")

        # **使用 `ThreadPoolExecutor` 进行多线程查询**
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(fetch_for_market, market_address_list)

    def read_signatures_file(self, symbol1, symbol2):
        """
        读取 `SIGNATURE_symbol1_symbol2.csv` 并返回符合 slot 过滤条件的交易签名
        """
        sig_file = os.path.join(CONFIG["output_path"], "SIGNATURE", f"{symbol1}_{symbol2}.csv")
        data_file1 = os.path.join(CONFIG["output_path"], "DATA", f"{symbol1}_{symbol2}.csv")
        data_file2 = os.path.join(CONFIG["output_path"], "DATA", f"{symbol2}_{symbol1}.csv")

        if not os.path.exists(sig_file):
            print(f"❌ 签名文件未找到: {sig_file}")
            return []

        print(f"🔍 读取交易签名文件: {sig_file}")

        # 读取 datafile1 和 datafile2，如果不存在则返回空 DataFrame
        def load_data_file(file_path):
            return pd.read_csv(file_path, usecols=["Signature"]) if os.path.exists(file_path) else pd.DataFrame(
                columns=["Signature"])

        df_data1 = load_data_file(data_file1)
        df_data2 = load_data_file(data_file2)

        # 合并 data_file1 和 data_file2 的 Signature
        existing_signatures = set(df_data1["Signature"]).union(set(df_data2["Signature"]))

        # 读取 sig_file 并进行过滤
        filtered_signatures = []
        with open(sig_file, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Slot"] is None:
                    continue
                slot = int(row["Slot"])
                signature = row["Signature"]

                # 过滤 slot 范围，并且 Signature 不能在 datafile 中已存在
                if self.start_slot <= slot <= self.end_slot and signature not in existing_signatures:
                    filtered_signatures.append((signature, row["Market_Address"]))

        return filtered_signatures

    def process_signatures_in_batches(self, tx_signatures):
        """
        多线程处理交易签名，并分配到不同的 LogDecoder（Solana RPC 端点）
        """
        if not tx_signatures:
            print("⚠️ 没有符合条件的交易签名，跳过解码！")
            return

        N = len(self.log_decoders) * 100  # 最大线程数
        total_tasks = len(tx_signatures)

        # **1️⃣ 创建全局进度条**
        global_progress = tqdm(total=total_tasks, desc="Overall Progress", position=0, leave=True, dynamic_ncols=True,
                               unit="tx")

        # **2️⃣ 计算批次数量**
        batch_size = max(1, total_tasks // N)
        batches = [tx_signatures[i:i + batch_size] for i in range(0, total_tasks, batch_size)]

        lock = threading.Lock()  # **线程安全锁**

        # **3️⃣ 线程池执行**
        with concurrent.futures.ThreadPoolExecutor(max_workers=N) as executor:
            futures = []

            def process_batch(batch, thread_id, log_decoder):
                """ 处理一个批次的交易签名，并使用指定的 log_decoder """
                start_time = time.time()
                print(f"\n✅ 已建立 {thread_id} 号线程，使用 RPC {log_decoder.solana_client._provider.endpoint_uri}")

                for transaction_signature, market_address in batch:
                    log_decoder.decode(transaction_signature, market_address)
                    with lock:
                        global_progress.update(1)

                elapsed_time = time.time() - start_time
                print(f"\n✅ 线程 {thread_id} 处理完成，共处理 {len(batch)} 笔交易，耗时 {elapsed_time:.2f} 秒")

            # **4️⃣ 提交任务**
            for idx, batch in enumerate(batches):
                log_decoder = self.log_decoders[idx % len(self.log_decoders)]  # 轮询选择不同的 RPC 节点
                futures.append(executor.submit(process_batch, batch, idx, log_decoder))

            # **5️⃣ 等待所有线程完成**
            concurrent.futures.wait(futures)
            global_progress.close()

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

            #
            self.print_stage_header("DECODING TX LOGS")
            tx_signatures = self.read_signatures_file(symbol1, symbol2)
            self.process_signatures_in_batches(tx_signatures)
            self.print_stage_header("DECODING TX SUCCESS")

# ========== 主函数 ========== #
if __name__ == "__main__":
    start_datetime = datetime.datetime(2025, 2, 27, 0, 0)
    end_datetime = datetime.datetime(2025, 2, 27, 1, 0)
    start_slot = 323278200
    end_slot = 323279200
    rpc_url = "https://wild-boldest-rain.solana-mainnet.quiknode.pro/b95f33839916945a42159c53ceab4d7468a51a69/"

    fetcher = SolanaFetcher(start_slot,end_slot,rpc_url)
    #fetcher = SolanaFetcher.from_datetime(start_datetime, end_datetime,rpc_url)
    fetcher.run()

