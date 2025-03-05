### **Solana 交易数据抓取与解析工具**

#### **项目简介**
本项目旨在提供一个完整的 Solana 交易数据抓取和解析工具，能够：
1. **获取流动性池信息** —— 通过 `RaydiumPoolFetcher` 查询 Solana 上 Raydium 交易对的池地址。
2. **获取交易签名** —— 通过 `TransactionFetcher` 获取指定时间段内某个市场地址的交易签名。
3. **解析交易详情** —— 通过 `LogDecoder` 解码交易日志，并计算非稳定币的相对价格。

#### **项目结构**
```
SOL_fetch/
│── INPUT/                   # 存放输入文件（如 `input.csv`）
│── RESULT/                  # 存放输出结果
│── samplecode/              # 示例代码
│── __pycache__/             # Python 编译缓存
│── config.py                # 配置文件
│── LogDecoder.py            # 交易日志解码器
│── RaydiumPoolFetcher.py    # 流动性池数据获取器
│── SolanaSlotFinder.py      # Slot 查询工具
│── SOL_fetcher.py           # 主要的执行逻辑
│── TransactionFetcher.py    # 交易签名抓取工具
│── __init__.py              # Python 模块初始化
```

---

### **安装与配置**
#### **1. 依赖安装**
确保你的环境安装了 Python 3，并使用 `venv` 创建虚拟环境：
```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

#### **2. 配置 `config.py`**
修改 `config.py`，定义 RPC 端点和输入/输出路径：
```python
CONFIG = {
    "rpc_url1": "https://api.mainnet-beta.solana.com",
    "rpc_url2": "https://solana-mainnet.rpcpool.com",
    "rpc_url3": "https://solana-api.projectserum.com",
    "input_path": "INPUT",
    "output_path": "RESULT",
}
```

---

### **使用方法**
#### **1. 准备输入文件**
在 `INPUT/input.csv` 文件中写入交易对，每行格式如下：
```
mint1,mint2
So11111111111111111111111111111111111111112,EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
...
```
其中：
- `mint1` 为第一个代币的合约地址（如 `SOL`）
- `mint2` 为第二个代币的合约地址（如 `USDC`）

#### **2. 运行主脚本**
执行 `SOL_fetcher.py`：
```bash
python SOL_fetcher.py
```
该脚本会：
1. 读取 `input.csv` 获取 `mint1, mint2` 交易对。
2. 获取流动性池 `POOL_symbol1_symbol2.csv` 并提取 `pool_id`。
3. 获取交易签名并存入 `SIGNATURE_symbol1_symbol2.csv`。
4. 解析交易日志，计算非稳定币的相对价格，并存入 `RESULT/DATA/`。

---

### **功能模块**
#### **1. `RaydiumPoolFetcher.py`**
用于获取 Raydium 交易池信息。
```python
fetcher = RaydiumPoolFetcher(mint1, mint2)
fetcher.run()
```
输出：
- `RESULT/POOL/POOL_symbol1_symbol2.csv`

#### **2. `TransactionFetcher.py`**
用于获取某个 `pool_id` 相关的交易签名。
```python
fetcher = TransactionFetcher(rpc_url, slot_finder, start_datetime, end_datetime, file_name)
fetcher.fetch_transactions(market_address)
```
输出：
- `RESULT/SIGNATURE/symbol1_symbol2.csv`

#### **3. `LogDecoder.py`**
用于解析交易日志，提取代币交易信息，并计算非稳定币价格。
```python
decoder = LogDecoder(rpc_url)
decoder.decode(transaction_signature, market_address, non_stable_symbol)
```
输出：
- `RESULT/DATA/symbol1_symbol2.csv`

---

### **示例**
假设 `input.csv` 里包含：
```
mint1,mint2
So11111111111111111111111111111111111111112,EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```
执行 `SOL_fetcher.py` 后，结果存储在：
```
RESULT/
│── POOL/
│   ├── POOL_SOL_USDC.csv  # 交易池信息
│── SIGNATURE/
│   ├── SOL_USDC.csv       # 交易签名
│── DATA/
│   ├── SOL_USDC.csv       # 交易解析结果
```
其中 `DATA/SOL_USDC.csv` 记录：
```
Signature, Non-Stable Change, Stable Change, Price, BlockTime
5x1Pq88Hq9G..., 2.5, 100, 40, 1740585600
```

---
### **后续改进**
- 支持更多交易所（如 Orca）
- 进一步优化查询效率
- 增加日志调试模式

---

### **许可证**
本项目开源，欢迎贡献优化。