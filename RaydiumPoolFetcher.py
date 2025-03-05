import requests
import json
import csv
import os
import config
from collections import defaultdict

CONFIG = config.CONFIG  # 直接使用 CONFIG


class RaydiumPoolFetcher:
    """
    通过 Raydium API 查询流动性池信息，并保存到 CSV 文件
    """

    RAYDIUM_API_BASE_URL = "https://api-v3.raydium.io"
    POOL_SEARCH_MINT = "/pools/info/mint"

    def __init__(self, mint1, mint2):
        """
        初始化流动性池查询类
        :param mint1: 代币1的mint地址（必填）
        :param mint2: 代币2的mint地址（必填）
        """
        self.mint1 = mint1
        self.mint2 = mint2
        self.mint1symbol = ""
        self.mint2symbol = ""

        # 内部锁定的查询参数
        self._pool_type = "all"
        self._sort = "liquidity"
        self._order = "desc"
        self._max_results = 100  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!默认最多获取 100 条数据
        self._page = 1

        # 结果文件保存路径
        self.output_path = os.path.join(CONFIG["output_path"], "POOL")
        os.makedirs(self.output_path, exist_ok=True)  # 确保目录存在

        # CSV 文件的路径，在获取数据后动态命名
        self.csv_file = None

    def fetch_pool_data(self):
        """
        从 Raydium API 获取流动性池数据
        """
        url = f"{self.RAYDIUM_API_BASE_URL}{self.POOL_SEARCH_MINT}"
        params = {
            "mint1": self.mint1,
            "mint2": self.mint2,
            "poolType": self._pool_type,
            "poolSortField": self._sort,
            "sortType": self._order,
            "pageSize": self._max_results,
            "page": self._page
        }

        try:
            print(f"🔍 发送请求到 Raydium API: {url}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  # 如果 HTTP 状态码非 200，抛出异常

            data = response.json()
            pools = data.get("data", {}).get("data", [])  # 获取 data.data 列表

            if not pools or not isinstance(pools, list):
                print("❌ API 数据格式错误或数据为空！")
                return []

            print(f"✅ 获取到 {len(pools)} 条流动性池数据，正在处理...")
            return pools

        except requests.exceptions.RequestException as e:
            print(f"❌ API 请求失败: {e}")
            return []

    def save_pools_to_csv(self, pools):
        """
        处理流动性池数据，并去重追加保存到 CSV 文件
        """
        if not pools:
            print("⚠️ 没有可用的数据，无需保存。")
            return

        # 提取数据 & 生成文件名
        records = []
        for pool in pools:
            if not isinstance(pool, dict):
                print(f"⚠️ 数据格式错误，跳过：{pool}")
                continue  # 跳过错误格式的数据

            pool_id = pool.get("id", "")
            mintA = pool.get("mintA", {})
            mintB = pool.get("mintB", {})
            mintA_address = mintA.get("address", "")
            mintA_symbol = mintA.get("symbol", "")
            mintB_address = mintB.get("address", "")
            mintB_symbol = mintB.get("symbol", "")

            # 生成 CSV 文件名
            if not self.csv_file:
                self.csv_file = os.path.join(self.output_path,f"POOL_{mintA_symbol}_{mintB_symbol}.csv")
                self.mint1symbol = mintA_symbol
                self.mint2symbol = mintB_symbol
            records.append((pool_id, mintA_address, mintA_symbol, mintB_address, mintB_symbol))


        # 读取已有数据，去重追加
        existing_data = self.load_existing_data()
        new_records = [r for r in records if r[0] not in existing_data]

        if not new_records:
            print("✅ 所有数据已存在，无需追加！")
            return

        # 追加数据到 CSV
        with open(self.csv_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # 如果文件为空，写入表头
            if os.stat(self.csv_file).st_size == 0:
                writer.writerow(["pool_id", "mintA_address", "mintA_symbol", "mintB_address", "mintB_symbol"])

            writer.writerows(new_records)

        print(f"📁 {len(new_records)} 条新数据已成功追加至 {self.csv_file}")

    def load_existing_data(self):
        """
        读取 CSV 现有数据，返回 pool_id 集合，避免重复写入
        """
        existing_ids = set()
        if self.csv_file and os.path.exists(self.csv_file):
            with open(self.csv_file, mode="r", encoding="utf-8") as file:
                reader = csv.reader(file)
                next(reader, None)  # 跳过表头
                for row in reader:
                    if row:
                        existing_ids.add(row[0])  # pool_id
        return existing_ids

    def run(self):
        """
        执行所有步骤：
        1. 获取数据
        2. 处理数据
        3. 去重并保存到 CSV
        """
        pools = self.fetch_pool_data()  # 获取数据
        self.save_pools_to_csv(pools)  # 处理并保存数据


if __name__ == "__main__":
    print("🔍 初始化 Raydium API 测试脚本...")

    # 目标 Token 对（示例：SOL/USDC）
    mint1 = "So11111111111111111111111111111111111111112"  # SOL
    mint2 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC

    print("📡 正在获取流动性池数据...")
    fetcher = RaydiumPoolFetcher(mint1, mint2)
    fetcher.run()  # 只需要调用 `run()`，自动完成所有步骤
