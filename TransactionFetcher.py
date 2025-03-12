import csv
import os
import datetime
import config
from solana.rpc.api import Client
from SolanaSlotFinder import SolanaSlotFinder
from solders.pubkey import Pubkey  # 导入 Pubkey
from solana.rpc.types import Commitment


CONFIG = config.CONFIG  # 直接使用 CONFIG


class TransactionFetcher:
    def __init__(self, rpc_url, slot_finder, start_slot, end_slot):
        """
        初始化交易查询器（不再绑定 file_name）
        """
        self.solana_client = Client(rpc_url)
        self.slot_finder = slot_finder
        self.start_slot = start_slot
        self.end_slot = end_slot

        # **文件输出目录**
        self.output_folder = os.path.join(CONFIG["output_path"], "SIGNATURE")
        os.makedirs(self.output_folder, exist_ok=True)  # 确保目录存在

        print(f"TransactionFetcher initialized with node: {rpc_url}")

    @classmethod
    def from_slots(cls, rpc_url, slot_finder, start_slot, end_slot):
        """
        通过 Slot 直接初始化（不需要时间戳）
        :param rpc_url: Solana RPC 端点
        :param slot_finder: SolanaSlotFinder 实例
        :param start_slot: 起始 Slot
        :param end_slot: 结束 Slot
        """
        instance = cls.__new__(cls)  # 直接创建实例，不调用 __init__
        instance.solana_client = Client(rpc_url)
        instance.slot_finder = slot_finder
        instance.start_slot = start_slot
        instance.end_slot = end_slot
        instance.start_datetime = None  # 不适用时间戳初始化
        instance.end_datetime = None
        instance.start_timestamp = None
        instance.end_timestamp = None

        # **文件输出目录**
        instance.output_folder = os.path.join(CONFIG["output_path"], "SIGNATURE")
        os.makedirs(instance.output_folder, exist_ok=True)  # 确保目录存在

        print(f"TransactionFetcher initialized with node: {rpc_url}")

        return instance  # **不在这里设置 `file_name`**

    def fetch_transactions_by_signature(self, market_pubkey, signature, limit, market_address):
        """
        获取交易签名，并返回获取到的交易数据中最旧的 slot。

        :param market_pubkey: 交易市场的公钥
        :param signature: 参考的交易签名
        :param limit: 获取交易的数量限制
        :param market_address: 市场地址
        :return: response.value 中最后一条交易的 slot，如果没有交易则返回 None
        """
        response = self.solana_client.get_signatures_for_address(
            market_pubkey,
            before=signature,
            limit=limit,
        )

        transactions = response.value
        if not transactions:
            print("⚠️ 没有找到更多的交易记录")
            return None  # 返回 None 以指示没有找到交易

        # 先保存数据
        self.save_transactions(transactions, self.start_slot, self.end_slot, market_address)

        # 返回最旧交易的 slot
        last_transaction_slot = transactions[-1].slot  # 获取最后一条交易的 slot
        last_transaction_signature = transactions[-1].signature
        if(last_transaction_slot >= self.start_slot and last_transaction_slot <= self.end_slot):
            self.fetch_transactions_by_signature(market_pubkey, last_transaction_signature, limit, market_address)
        else:
            return


    def fetch_transactions(self, market_address,file_name,limit=1000):
        """
        查询指定时间范围内的交易，并存入 CSV（去重插入）
        :param market_address: 目标账户地址
        :param limit: 每次查询的最大交易数
        """
        market_pubkey = Pubkey.from_string(market_address)  # 在方法内解析 market_address
        self.output_file = os.path.join(self.output_folder, file_name)  # **动态设置输出文件路径**

        print(f"Fetching transactions from Market Address: {market_address}")

        # 获取 Slot 的交易
        response = self.solana_client.get_block(
            self.end_slot,
            encoding="json",
            max_supported_transaction_version=0  # 解决版本不兼容问题
        )

        transactions = response.value.transactions
        signatures = [txn.transaction.signatures[0] for txn in transactions if hasattr(txn.transaction, "signatures")]

        if not signatures:
            print("⚠️ No transactions found in the block.")
            return

        first_signature = signatures[0]

        # 获取 `first_signature` 之前的交易签名

        self.fetch_transactions_by_signature(market_pubkey, first_signature, limit, market_address)


        # 处理并存储交易数据
        #self.save_transactions(response.value, self.start_slot, self.end_slot, market_address)


    def save_transactions(self, transactions, start_slot, end_slot, market_address):
        """
        读取已有交易数据，去重后插入新交易
        :param transactions: Solana 交易列表
        :param start_slot: 起始 Slot
        :param end_slot: 结束 Slot
        :param market_address: 当前交易市场地址
        """
        if not transactions:
            print("⚠️ No transactions found.")
            return 0

        # 读取已有交易签名，避免重复插入
        existing_signatures = set()
        if os.path.exists(self.output_file):
            with open(self.output_file, mode='r', newline='') as file:
                reader = csv.reader(file)
                next(reader, None)  # 跳过 CSV 头部
                existing_signatures = {row[0] for row in reader}  # 读取已有交易的 signature

        # 追加模式写入新交易
        new_entries = 0
        last_slot = None  # 记录 Slot 范围

        with open(self.output_file, mode='a', newline='') as file:
            writer = csv.writer(file)

            # 如果文件为空，先写入头部
            if os.stat(self.output_file).st_size == 0:
                writer.writerow(["Signature", "Slot", "Market_Address"])  # 新增 `Market Address`

            # 遍历 transactions 并仅存储交易成功 & slot 在范围内的记录
            for txn in transactions:
                sig = str(txn.signature).strip().replace("\n", "").replace("Signature(", "").replace(")",
"")  # **处理转义符**
                last_slot = txn.slot  # 更新最后一条交易的 slot
                if txn.err is None:
                    if start_slot <= txn.slot <= end_slot:
                        if sig not in existing_signatures:
                            writer.writerow([txn.signature, txn.slot, market_address])
                            new_entries += 1

        # 打印存储信息
        print(f"✅ {new_entries} new transactions saved to {self.output_file} | Last Slot :{last_slot}")
        return new_entries


# ========== 使用示例 ==========
if __name__ == "__main__":
    rpc_url = "https://wild-boldest-rain.solana-mainnet.quiknode.pro/b95f33839916945a42159c53ceab4d7468a51a69/"
    slot_finder = SolanaSlotFinder(rpc_url)
    start_datetime = datetime.datetime(2025, 2, 27, 0, 0)
    end_datetime = datetime.datetime(2025, 2, 27, 0, 1)
    file_name = "SOL_USDC.csv"  # 这里手动指定文件名

    # 传递不同的 market_address 查询交易
    market_address = "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"
    #fetcher.fetch_transactions(market_address)

    start_slot = 323247000
    end_slot = 323247500

    fetcher = TransactionFetcher.from_slots(rpc_url, slot_finder, start_slot, end_slot)
    fetcher.fetch_transactions(market_address,file_name)
