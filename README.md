# Binance-Deribit BTC 跨所套利自动化交易程序

这是一个面向 Deribit 期权与 Binance USDT-M 永续合约的跨交易所套利/对冲项目。项目包含套利扫描、下单执行、持仓监控、结算处理、Redis 状态恢复、Telegram 告警与 Web 监控面板。

> 风险提示：本项目涉及交易所 API 和自动交易逻辑。请先使用测试网、小资金、只读或低权限 API Key 做充分验证。任何自动交易策略都可能因网络、交易所接口、行情波动、配置错误或代码缺陷造成亏损。

实时交流平台：https://linux.do/t/topic/2323516

本项目由 https://num.cc 提供算力支持

## 功能概览

- 扫描 Deribit BTC 期权与 Binance 永续合约之间的潜在套利机会。
- 使用 Redis 保存运行状态，支持进程重启后的状态恢复。
- 使用 SQLite 记录交易、账户权益和浮亏峰值等运行数据。
- 支持 Telegram 机器人查看状态、暂停、恢复、调整部分参数和接收告警。
- 提供 Flask Web 监控面板查看持仓、日志、统计和账户权益。
- 启动前执行 preflight 检查，确认 `.env`、API Key、Redis、交易所 REST、持仓和磁盘空间。
- 默认配置为测试网，实盘前需要明确修改配置并重新检查风控参数。

## 目录结构

```text
.
├── binance_deribit.py        # 主程序入口
├── binance-monitor.py        # Web 监控面板
├── config.py                 # 策略、交易所、风控配置
├── binance_futures.py        # Binance 期货接口
├── deribit_client.py         # Deribit WebSocket/REST 客户端
├── trade_executor.py         # 下单执行逻辑
├── telegram_handler.py       # Telegram 命令和告警
├── db_store.py               # SQLite 持久化
├── engine/                   # 核心引擎 mixin 模块
├── .env.example              # 环境变量模板
├── requirements.txt          # Python 依赖
├── pipinstall.txt            # 原始安装命令备忘
└── README.md
```

## 部署环境要求

推荐环境：

- Python 3.10 或更高版本
- Redis 6 或更高版本
- macOS 或 Ubuntu Linux
- 可访问 Deribit、Binance Futures、Telegram API 的网络环境

Python 依赖：

- `aiohttp`
- `flask`
- `orjson`
- `redis`
- `requests`
- `websockets`

## 快速部署

### 1. 克隆代码

```bash
git clone https://github.com/beijingcao/binance-deribit-btc.git
cd binance-deribit-btc
```

### 2. 创建 Python 虚拟环境

macOS / Linux：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

如果服务器同时安装了多个 Python 版本，请确认 `python --version` 指向你准备运行项目的版本。

### 3. 安装并启动 Redis

macOS Homebrew：

```bash
brew install redis
brew services start redis
redis-cli ping
```

Ubuntu / Debian：

```bash
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable redis-server
sudo systemctl start redis-server
redis-cli ping
```

如果返回 `PONG`，说明 Redis 已正常运行。

### 4. 配置环境变量

复制模板：

```bash
cp .env.example .env
```

编辑 `.env`：

```bash
nano .env
```

需要填写的字段：

```bash
TG_BOT_TOKEN=
TG_CHAT_ID=
TG_ALLOWED_USER_IDS=

DERIBIT_BTC_CLIENT_ID=
DERIBIT_BTC_CLIENT_SECRET=

DERIBIT_ETH_CLIENT_ID=
DERIBIT_ETH_CLIENT_SECRET=

BINANCE_API_KEY=
BINANCE_API_SECRET=
```

说明：

- `TG_BOT_TOKEN`：Telegram Bot Token，用于告警和命令交互。
- `TG_CHAT_ID`：允许机器人发送消息的 Telegram chat id。
- `TG_ALLOWED_USER_IDS`：可选，限制可发送命令的 Telegram user id，多个用英文逗号分隔。
- `DERIBIT_BTC_CLIENT_ID` / `DERIBIT_BTC_CLIENT_SECRET`：Deribit BTC 账户 API 凭证。
- `BINANCE_API_KEY` / `BINANCE_API_SECRET`：Binance USDT-M Futures API 凭证。

`.env` 已被 `.gitignore` 忽略，不能提交到 GitHub。

### 5. 检查策略和环境配置

打开 `config.py`，重点确认：

```python
BASE_CONFIG = {
    "target_currency": "BTC",
    "test_trading": True,
}

BINANCE_CONFIG = {
    "use_testnet": True,
    "use_hedge_mode": True,
    "leverage": 20,
    "margin_type": "ISOLATED",
}
```

测试网运行时：

- `BASE_CONFIG["test_trading"] = True`
- `BINANCE_CONFIG["use_testnet"] = True`
- 使用 Deribit 测试网和 Binance Futures 测试网 API Key

实盘运行前：

- 将 `BASE_CONFIG["test_trading"]` 改为 `False`
- 将 `BINANCE_CONFIG["use_testnet"]` 改为 `False`
- 重新确认 API Key 权限、杠杆、保证金模式、交易量、止损、利润门槛和风控参数

## 启动主程序

确保虚拟环境和 Redis 已启动：

```bash
source .venv/bin/activate
redis-cli ping
python binance_deribit.py
```

程序启动时会执行 preflight 检查。关键检查失败时会拒绝启动，常见原因包括：

- `.env` 文件不存在
- Deribit 或 Binance API Key 未配置
- Redis 未启动
- 交易所 REST 接口不可访问
- API Key 认证失败
- 磁盘空间不足

如果确认当前没有持仓，并且只想在关键检查失败时强制启动，可使用：

```bash
python binance_deribit.py --force-startup
```

注意：如果检测到交易所有持仓，`--force-startup` 也不会绕过关键保护。

## 启动监控面板

另开一个终端：

```bash
source .venv/bin/activate
python binance-monitor.py --host 127.0.0.1 --port 5556
```

浏览器访问：

```text
http://127.0.0.1:5556
```

默认登录信息：

```text
用户名：admin
密码：123456
```

首次登录后请立即进入 `/change_password` 修改默认密码。监控面板会在本地生成 `.monitor_auth.json`，该文件包含密码哈希和 session secret，已经被 `.gitignore` 忽略。

如果需要让局域网或公网访问监控面板，可以把 `--host` 改成 `0.0.0.0`，但必须配合防火墙、反向代理、HTTPS 和强密码，不建议直接裸露到公网。

## 后台运行建议

### 使用 tmux

```bash
tmux new -s arb
source .venv/bin/activate
python binance_deribit.py
```

按 `Ctrl+b` 后按 `d` 可以退出 tmux 会话但保持程序运行。

重新进入：

```bash
tmux attach -t arb
```

监控面板可以使用另一个 tmux 会话：

```bash
tmux new -s arb-monitor
source .venv/bin/activate
python binance-monitor.py --host 127.0.0.1 --port 5556
```

### 使用 systemd（Ubuntu）

示例主程序服务：

```ini
[Unit]
Description=Binance Deribit Arbitrage Bot
After=network-online.target redis-server.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/binance-deribit-btc
ExecStart=/opt/binance-deribit-btc/.venv/bin/python /opt/binance-deribit-btc/binance_deribit.py
Restart=always
RestartSec=5
User=ubuntu
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

保存为：

```bash
sudo nano /etc/systemd/system/binance-deribit.service
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable binance-deribit
sudo systemctl start binance-deribit
sudo systemctl status binance-deribit
```

查看日志：

```bash
journalctl -u binance-deribit -f
```

如果项目目录、用户或 Python 路径不同，请同步修改 `WorkingDirectory`、`ExecStart` 和 `User`。

## 常用运维命令

查看 Git 状态：

```bash
git status
```

查看 Redis 状态：

```bash
redis-cli ping
redis-cli -n 2 keys '*'
```

测试网 BTC 默认使用 Redis db 2。代码里的映射为：

```text
mainnet BTC  -> db 0
mainnet ETH  -> db 1
testnet BTC  -> db 2
testnet ETH  -> db 3
```

查看主程序日志文件：

```bash
tail -f BTC-log.txt
```

查看监控面板：

```bash
open http://127.0.0.1:5556
```

Linux 服务器可在本机访问：

```bash
curl -I http://127.0.0.1:5556
```

## 更新代码

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt
```

如果使用 systemd：

```bash
sudo systemctl restart binance-deribit
sudo systemctl status binance-deribit
```

## 安全注意事项

- 不要提交 `.env`、真实 API Key、账户截图、交易日志和数据库文件。
- Binance API Key 建议限制 IP，并只开启需要的合约交易权限。
- Deribit API Key 建议按环境区分测试网和实盘。
- 实盘前先确认 `test_trading` 和 `use_testnet` 是否已经按预期切换。
- 监控面板默认密码必须修改。
- 不建议直接把监控面板暴露到公网。
- 自动交易前请确认止损、交易量、杠杆、保证金模式和最大持仓数量。

## 开源协议

本项目使用 MIT License。详情请查看 [LICENSE](LICENSE)。
