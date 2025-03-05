import datetime
import time
from solana.rpc.api import Client
from solana.rpc.core import RPCException

class SolanaSlotFinder:
    def __init__(self, rpc_url):
        """
        初始化 Solana 客户端
        """
        self.solana_client = Client(rpc_url)
        print(f"SolanaSlotFinder initial success by node: {rpc_url}",)

    def get_latest_slot(self):
        """
        获取最新的 Slot，确保返回整数值。
        """
        latest_slot_response = self.solana_client.get_slot()
        if hasattr(latest_slot_response, "value"):
            return latest_slot_response.value
        raise ValueError(f"Unexpected get_slot() response: {latest_slot_response}")

    def get_block_time(self, slot):
        """
        获取某个 slot 的 Unix 时间戳，并处理跳过的 Slot 错误。
        """
        time.sleep(0.02)
        try:
            result = self.solana_client.get_block_time(slot)
            if hasattr(result, "value") and isinstance(result.value, int):
                return result.value
        except RPCException as e:
            if "SlotSkippedMessage" in str(e):
                return None  # 跳过该 Slot
        return None  # 未找到时间信息

    def find_closest_slot(self, target_timestamp):
        """
        使用二分查找找到最接近目标时间的 Solana Slot。
        :param target_timestamp: 目标时间（Unix 时间戳）
        :return: 最接近的 slot
        """
        print("find_closest_slot of:", target_timestamp)
        latest_slot = self.get_latest_slot()
        start_slot, end_slot = 1, latest_slot
        closest_slot, closest_time_diff = None, float("inf")

        while start_slot <= end_slot:
            mid_slot = (start_slot + end_slot) // 2
            mid_time = self.get_block_time(mid_slot)

            if mid_time is None:
                end_slot = mid_slot - 1  # 跳过无效 Slot
                continue

            time_diff = abs(mid_time - target_timestamp)
            if time_diff < closest_time_diff:
                closest_time_diff, closest_slot = time_diff, mid_slot

            if mid_time < target_timestamp:
                start_slot = mid_slot + 1
            else:
                end_slot = mid_slot - 1

        print(f"Closest slot to {target_timestamp}: {closest_slot}")
        return closest_slot

# 使用示例
if __name__ == "__main__":
    rpc_url = "https://wild-boldest-rain.solana-mainnet.quiknode.pro/b95f33839916945a42159c53ceab4d7468a51a69/"
    slot_finder = SolanaSlotFinder(rpc_url)

    # 输入时间并查找最接近的 Slot
    target_datetime = datetime.datetime(2025, 2, 27, 0, 0)
    target_timestamp = int(target_datetime.timestamp())

    closest_slot = slot_finder.find_closest_slot(target_timestamp)
    print(f"Closest slot to {target_datetime}: {closest_slot}")
