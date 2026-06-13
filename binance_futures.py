# binance_futures.py
# Binance USDT-M 期货模块：认证、费率、合约匹配、WebSocket 客户端、订单执行
import orjson
import asyncio
import time
import random
import hmac
import hashlib
import logging
import aiohttp
import websockets
from decimal import Decimal, getcontext, ROUND_DOWN
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field

# 设置Decimal精度
getcontext().prec = 28

logger = logging.getLogger(__name__)

# ================= 🌟 超高速 JSON 引擎 (orjson 封装) =================
class FastJSON:
    @staticmethod
    def loads(obj):
        return orjson.loads(obj)

    @staticmethod
    def dumps(obj):
        # orjson.dumps 返回的是 bytes，我们解码为字符串以兼容原生 WebSocket
        return orjson.dumps(obj).decode('utf-8')

json = FastJSON()  # 覆盖系统原生的 json 模块


# ================= Binance HMAC SHA256 认证 =================
class BinanceAuth:
    """Binance API 认证：HMAC SHA256 签名 + 端点管理"""

    # 生产环境端点
    REST_BASE = "https://fapi.binance.com"
    REST_BASE_TESTNET = "https://demo-fapi.binance.com"
    WS_BASE = "wss://fstream.binance.com"
    WS_BASE_TESTNET = "wss://fstream.binancefuture.com"

    def __init__(self, api_key: str, api_secret: str, is_testnet: bool = True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.is_testnet = is_testnet

        # 根据模式选择端点
        if is_testnet:
            self.rest_base = self.REST_BASE_TESTNET
            self.ws_base = self.WS_BASE_TESTNET
        else:
            self.rest_base = self.REST_BASE
            self.ws_base = self.WS_BASE

        logger.info(f"[Binance认证] 模式={'测试网' if is_testnet else '生产环境'}, REST={self.rest_base}")

        # 🌟 F/A5 修复: 本地时钟与 Binance serverTime 的漂移 (毫秒)
        # = serverTime_ms - localTime_ms; sign() 用 (localTime + offset) 避免 -1022 签名过期
        self._server_time_offset_ms: int = 0
        self._server_time_synced_at: float = 0.0     # 上次同步的本地时间戳 (秒)
        self._server_time_resync_interval: float = 3600  # 每小时重同步一次

    async def sync_server_time(self) -> bool:
        """从 Binance /fapi/v1/time 拉取服务器时间, 计算本地漂移。

        启动时调用; 之后每小时自动重同步以抵抗本地时钟慢速漂移。
        漂移 > 2 秒告警 (Binance recvWindow 默认 5 秒)。
        """
        try:
            import aiohttp as _aiohttp
            _url = f"{self.rest_base}/fapi/v1/time"
            async with _aiohttp.ClientSession() as _sess:
                async with _sess.get(_url, timeout=5) as _resp:
                    if _resp.status != 200:
                        logger.warning(f"[Binance认证] serverTime 拉取失败 HTTP={_resp.status}")
                        return False
                    _data = await _resp.json()
                    _server_ms = int(_data.get('serverTime', 0))
                    if _server_ms <= 0:
                        return False
                    _local_ms = int(time.time() * 1000)
                    _offset = _server_ms - _local_ms
                    _abs = abs(_offset)
                    self._server_time_offset_ms = _offset
                    self._server_time_synced_at = time.time()
                    if _abs > 2000:
                        logger.warning(
                            f"[Binance认证] ⚠️ 本地时钟漂移 {_offset} ms (>2s), "
                            f"已修正; 建议检查 NTP")
                    else:
                        logger.info(f"[Binance认证] serverTime 已同步, 漂移={_offset} ms")
                    return True
        except Exception as _e:
            logger.warning(f"[Binance认证] serverTime 同步异常: {_e}")
            return False

    def sign(self, params: dict) -> dict:
        """对请求参数添加时间戳和 HMAC SHA256 签名
        🌟 F/A5 修复: 使用 (local + server_offset) 作为时间戳, 抵抗本地时钟漂移
        """
        params.setdefault('recvWindow', 10000)
        _ts_ms = int(time.time() * 1000) + self._server_time_offset_ms
        params['timestamp'] = _ts_ms
        params.pop('signature', None)
        query_string = '&'.join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        params['signature'] = signature
        return params

    @property
    def headers(self) -> dict:
        """返回带 API Key 的请求头"""
        return {"X-MBX-APIKEY": self.api_key}

    def build_public_stream_url(self, stream_path: str) -> str:
        """构建 Public 类市场流 URL。

        Binance USD-M 生产环境已将 depth 等 public streams 路由到 /public。
        测试网保持 legacy /stream，避免测试网未同步新路由时误伤。
        """
        route = "stream" if self.is_testnet else "public/stream"
        return f"{self.ws_base}/{route}?streams={stream_path}"

    def build_market_stream_url(self, stream_path: str) -> str:
        """构建 Market 类市场流 URL，如 markPrice / aggTrade。"""
        route = "stream" if self.is_testnet else "market/stream"
        return f"{self.ws_base}/{route}?streams={stream_path}"


# ================= Binance 期货费率计算器 =================
class BinanceFeeCalculator:
    """Binance USDT-M 期货费率计算器（费率以 USDT 计价，需转换为 BTC）"""

    # 费率结构：{tier: (maker_rate, taker_rate)}
    FEE_TIERS = {
        "standard": (Decimal('0.0002'), Decimal('0.0004')),   # 标准：maker 0.02%, taker 0.04%
        "vip1":     (Decimal('0.00016'), Decimal('0.0004')),  # VIP1：maker 0.016%, taker 0.04%
    }

    def __init__(self, tier: str = "standard"):
        if tier not in self.FEE_TIERS:
            logger.warning(f"[Binance费率] 未知费率等级 '{tier}'，使用 standard")
            tier = "standard"
        self.tier = tier
        self.maker_rate, self.taker_rate = self.FEE_TIERS[tier]
        logger.info(f"[Binance费率] 等级={tier}, maker={self.maker_rate}, taker={self.taker_rate}")

    @staticmethod
    def calculate_fee_usdt(price: Decimal, quantity: Decimal, is_taker: bool = True,
                           tier: str = "standard") -> Decimal:
        """计算手续费 (USDT) = price * quantity * rate"""
        rates = BinanceFeeCalculator.FEE_TIERS.get(tier, BinanceFeeCalculator.FEE_TIERS["standard"])
        rate = rates[1] if is_taker else rates[0]
        return price * quantity * rate

    @staticmethod
    def calculate_fee_btc(price: Decimal, quantity: Decimal, is_taker: bool = True,
                          tier: str = "standard") -> Decimal:
        """计算手续费 (BTC) = fee_usdt / price"""
        fee_usdt = BinanceFeeCalculator.calculate_fee_usdt(price, quantity, is_taker, tier)
        if price == 0:
            return Decimal('0')
        return fee_usdt / price

    @staticmethod
    def estimate_funding_cost_usdt(position_value_usdt: Decimal, funding_rate: Decimal,
                                   hold_hours: int = 48) -> Decimal:
        """估算资金费用 (USDT)：funding 每 8 小时结算一次"""
        # 持仓时间内的 funding 结算次数
        funding_periods = Decimal(str(hold_hours)) / Decimal('8')
        return position_value_usdt * funding_rate * funding_periods


# ================= Binance 期货合约匹配器 =================
class BinanceFuturesMatcher:
    """将 Deribit 期权到期日匹配到 Binance 永续合约 (BTCUSDT)

    系统仅使用永续合约，不再支持可交割合约。
    """

    PERPETUAL_SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}

    def __init__(self, priority: str = "perpetual", currency: str = "BTC"):
        """
        Args:
            priority: 保留参数，忽略输入值，始终使用 perpetual
            currency: 目标币种 - "BTC" 或 "ETH"
        """
        self.priority = "perpetual"
        self.perpetual_symbol = self.PERPETUAL_SYMBOLS.get(currency.upper(), f"{currency.upper()}USDT")
        # 保留属性以兼容外部代码
        self.deliverable_contracts: Dict[str, str] = {}
        logger.info(f"[合约匹配] 仅使用永续合约 {self.perpetual_symbol}")

    def update_available_contracts(self, binance_symbols: List[dict] = None):
        """保留方法签名以兼容外部调用，不再扫描可交割合约

        Args:
            binance_symbols: 忽略，仅使用永续合约
        """
        logger.info(f"[合约匹配] 仅使用永续合约 {self.perpetual_symbol}，跳过可交割合约扫描")

    def _binance_to_deribit_expiry(self, yymmdd: str) -> Optional[str]:
        """已废弃: 不再使用可交割合约，保留方法以兼容外部调用"""
        return None

    def _deribit_to_binance_expiry(self, deribit_expiry: str) -> Optional[str]:
        """已废弃: 不再使用可交割合约，保留方法以兼容外部调用"""
        return None

    def match(self, deribit_expiry: str) -> Tuple[str, str]:
        """匹配 Binance 合约 — 始终返回永续合约

        Args:
            deribit_expiry: Deribit 格式的到期日（忽略，始终使用永续）

        Returns:
            (binance_symbol, "perpetual")
        """
        return (self.perpetual_symbol, "perpetual")


# ================= Binance 订单簿 (数据类) =================
@dataclass
class BinanceOrderBook:
    """Binance 期货订单簿快照"""
    symbol: str = ""
    bids: List[List[Decimal]] = field(default_factory=list)  # [[price, qty], ...]
    asks: List[List[Decimal]] = field(default_factory=list)  # [[price, qty], ...]
    update_time: float = 0.0

    @property
    def best_bid(self) -> Optional[Decimal]:
        """最优买价"""
        if self.bids:
            return self.bids[0][0]
        return None

    @property
    def best_ask(self) -> Optional[Decimal]:
        """最优卖价"""
        if self.asks:
            return self.asks[0][0]
        return None

    @property
    def mid_price(self) -> Optional[Decimal]:
        """中间价"""
        bid = self.best_bid
        ask = self.best_ask
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return None


# ================= Binance 期货持仓 (数据类) =================
@dataclass
class BinanceFuturesPosition:
    """Binance 期货持仓信息"""
    symbol: str = ""
    side: str = ""  # "LONG" 或 "SHORT"
    position_side: str = "BOTH"  # "BOTH"(单向) / "LONG" / "SHORT"(双向)
    quantity: Decimal = Decimal('0')
    entry_price: Decimal = Decimal('0')
    unrealized_pnl: Decimal = Decimal('0')
    mark_price: Decimal = Decimal('0')


# ================= Binance 期货 WebSocket 客户端 =================
class BinanceFuturesWSClient:
    """Binance USDT-M 期货 WebSocket 客户端

    功能：
    - 市场数据：深度行情、标记价格、逐笔成交
    - 用户数据：账户更新、订单更新
    - REST 辅助：交易所信息查询、持仓风险查询
    """

    def __init__(self, auth: BinanceAuth):
        self.auth = auth

        # 数据存储
        self.order_books: Dict[str, BinanceOrderBook] = {}
        self.mark_prices: Dict[str, Decimal] = {}
        self.mark_price_update_times: Dict[str, float] = {}
        self.funding_rates: Dict[str, Decimal] = {}
        self.last_prices: Dict[str, Decimal] = {}
        self.last_price_update_times: Dict[str, float] = {}
        self.positions_by_side: Dict[Tuple[str, str], BinanceFuturesPosition] = {}
        self.positions: Dict[str, BinanceFuturesPosition] = {}
        self.balances: Dict[str, Decimal] = {}  # {asset: available_balance}
        self._order_updates: asyncio.Queue = asyncio.Queue()
        self.dual_side_mode: bool = False

        # WebSocket 连接
        self._market_ws = None
        self._public_ws = None
        self._regular_market_ws = None
        self._user_ws = None
        self._listen_key: Optional[str] = None

        # aiohttp 会话
        self._session: Optional[aiohttp.ClientSession] = None

        # 控制标志
        self._running = False
        self._connected_market = asyncio.Event()
        self._connected_public_market = asyncio.Event()
        self._connected_regular_market = asyncio.Event()
        self._connected_user = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._user_reconnect_lock = asyncio.Lock()
        self.market_connected_at: float = 0.0
        self.on_market_disconnect: Optional[Callable[[str], None]] = None
        # 🌟 2026-06-11 幻影空仓事故修复: 持仓数据可信度追踪
        # user_data_ready_at: 用户数据流就绪时刻 (重连后重置)
        # position_snapshot_ok/at: 初始持仓快照是否成功 + 时刻
        # position_zero_events: 交易所主动推送的持仓归零事件 (ACCOUNT_UPDATE),
        #   是"持仓真的没了"的因果证据 — 区别于 REST 假空读数
        self.user_data_ready_at: float = 0.0
        self.position_snapshot_ok: bool = False
        self.position_snapshot_at: float = 0.0
        self._initial_snapshot_in_progress: bool = False
        self.position_zero_events: Dict[Tuple[str, str], float] = {}
        # ORDER_TRADE_UPDATE 推送的成交均价缓存 (orderId -> avgPrice),
        # 用于测试网 MARKET 单响应 avgPrice=0 时的零成本回填
        self.order_avg_prices: Dict[str, Decimal] = {}

        # 订阅的 symbol 列表
        self._subscribed_symbols: List[str] = []
        self._first_market_stream_seen: set = set()

        logger.info("[Binance WS] 客户端初始化完成")

    @property
    def connected(self) -> bool:
        """综合连接状态：市场 WS + 用户数据 WS 均已连接才视为就绪"""
        return self._connected_market.is_set() and self._connected_user.is_set()

    def _refresh_market_connected_state(self) -> None:
        """Public depth 与 Market mark/last 两路都就绪时，整体市场数据才算就绪。"""
        if self._connected_public_market.is_set() and self._connected_regular_market.is_set():
            self._connected_market.set()
            if not self.market_connected_at:
                self.market_connected_at = time.time()
        else:
            self._connected_market.clear()
            self.market_connected_at = 0.0

    def _mark_market_disconnected(self, feed: str = "all", reason: str = "") -> None:
        """市场 WS 失效时立即清理对应行情并通知上层暂停开仓。"""
        if feed not in ("public", "market", "all"):
            reason = feed
            feed = "all"
        if feed in ("public", "all"):
            self._public_ws = None
            self._connected_public_market.clear()
            self.order_books.clear()
        if feed in ("market", "all"):
            self._regular_market_ws = None
            self._connected_regular_market.clear()
            self.mark_prices.clear()
            self.mark_price_update_times.clear()
            self.last_prices.clear()
            self.last_price_update_times.clear()
            self.funding_rates.clear()
        self._market_ws = None
        self._refresh_market_connected_state()
        if self.on_market_disconnect:
            try:
                self.on_market_disconnect(reason or feed)
            except Exception as _cb_err:
                logger.warning(f"[Binance WS] 市场断线回调异常: {type(_cb_err).__name__} {repr(_cb_err)}")

    # ============ REST 请求辅助 ============

    async def _ensure_session(self):
        """确保 aiohttp 会话已创建"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self.auth.headers,
                timeout=aiohttp.ClientTimeout(total=10)
            )

    async def _rest_request(self, method: str, path: str, params: dict = None,
                            signed: bool = False) -> Any:
        """发送 REST 请求

        Args:
            method: "GET", "POST", "PUT", "DELETE"
            path: API 路径，如 "/fapi/v1/order"
            params: 请求参数
            signed: 是否需要签名

        Returns:
            解析后的 JSON 响应
        """
        await self._ensure_session()

        if params is None:
            params = {}

        url = f"{self.auth.rest_base}{path}"
        _method = method.upper()
        if _method not in {"GET", "POST", "PUT", "DELETE"}:
            logger.error(f"[Binance REST] 不支持的 HTTP 方法: {method}")
            return None

        _max_retries = 3
        _retriable_status = {418, 429, 500, 502, 503, 504}
        for _attempt in range(_max_retries):
            _params = dict(params or {})
            if signed:
                _params = self.auth.sign(_params)

            try:
                if _method == "GET":
                    _req_ctx = self._session.get(url, params=_params)
                elif _method == "POST":
                    _req_ctx = self._session.post(url, params=_params)
                elif _method == "PUT":
                    _req_ctx = self._session.put(url, params=_params)
                else:
                    _req_ctx = self._session.delete(url, params=_params)

                async with _req_ctx as resp:
                    data = await resp.read()
                    _content_type = resp.headers.get('Content-Type', '')
                    _body_head = data[:200].decode('utf-8', errors='replace').replace('\n', ' ') if data else ''
                    _looks_json = (
                        not data or
                        'json' in _content_type.lower() or
                        data.lstrip().startswith((b'{', b'['))
                    )
                    if _looks_json:
                        try:
                            result = orjson.loads(data) if data else {}
                        except Exception as _json_err:
                            _can_retry = (resp.status in _retriable_status and _attempt < _max_retries - 1)
                            _msg = (
                                f"[Binance REST] {method} {path} JSON解析失败 HTTP={resp.status} "
                                f"content-type={_content_type or '-'} err={_json_err} body={_body_head!r}"
                            )
                            if _can_retry:
                                _delay = min(2.0, 0.4 * (2 ** _attempt)) + random.uniform(0, 0.2)
                                logger.warning(f"{_msg}，第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                                await asyncio.sleep(_delay)
                                continue
                            logger.error(_msg)
                            return None
                    else:
                        _can_retry = (resp.status in _retriable_status and _attempt < _max_retries - 1)
                        _msg = (
                            f"[Binance REST] {method} {path} 非JSON响应 HTTP={resp.status} "
                            f"content-type={_content_type or '-'} body={_body_head!r}"
                        )
                        if _can_retry:
                            _delay = min(2.0, 0.4 * (2 ** _attempt)) + random.uniform(0, 0.2)
                            logger.warning(f"{_msg}，第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                            await asyncio.sleep(_delay)
                            continue
                        logger.error(_msg)
                        return None
                    if resp.status == 200:
                        return result

                    _err_code = result.get('code') if isinstance(result, dict) else None
                    if signed and _err_code == -1021 and _attempt < _max_retries - 1:
                        await self.auth.sync_server_time()
                        _delay = min(1.0, 0.2 * (2 ** _attempt)) + random.uniform(0, 0.1)
                        logger.warning(
                            f"[Binance REST] {method} {path} timestamp 超出 recvWindow，"
                            f"已重同步 serverTime，第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                        await asyncio.sleep(_delay)
                        continue

                    _can_retry = (resp.status in _retriable_status and _attempt < _max_retries - 1)
                    if _can_retry:
                        _delay = min(2.0, 0.4 * (2 ** _attempt)) + random.uniform(0, 0.2)
                        logger.warning(
                            f"[Binance REST] {method} {path} 状态码={resp.status}，"
                            f"第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                        await asyncio.sleep(_delay)
                        continue

                    # 🌟 良性错误码白名单: 这些是 Binance 用 HTTP 400 表达"已是目标状态/重复请求"
                    # 等非故障情况, 上层已正确识别并视为成功, 此处降级为 INFO 避免误报 ERROR
                    _benign_codes = {
                        -4046,  # "No need to change margin type." (已是目标模式)
                        -4059,  # "No need to change position side." (持仓方向已匹配)
                        -4048,  # "Margin type cannot be changed" 持仓时切换 — 是上层需要感知的失败, 但不应算作 REST 层 ERROR
                        -5022,  # Duplicate order id — 上层有 _query_order_by_client_oid 专门处理
                    }
                    if _err_code in _benign_codes:
                        logger.info(
                            f"[Binance REST] {method} {path} 返回良性状态 {resp.status} "
                            f"code={_err_code} msg={result.get('msg', '')}")
                    else:
                        logger.error(f"[Binance REST] {method} {path} 失败: {resp.status} {result}")
                    return result

            except aiohttp.ClientError as e:
                _err_detail = f"{type(e).__name__} {repr(e)}"
                if _attempt < _max_retries - 1:
                    _delay = min(2.0, 0.4 * (2 ** _attempt)) + random.uniform(0, 0.2)
                    logger.warning(
                        f"[Binance REST] 请求异常 {method} {path}: {_err_detail}，"
                        f"第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                    await asyncio.sleep(_delay)
                    continue
                logger.error(f"[Binance REST] 请求异常 {method} {path}: {_err_detail}")
                return None
            except Exception as e:
                _err_detail = f"{type(e).__name__} {repr(e)}"
                if _attempt < _max_retries - 1:
                    _delay = min(2.0, 0.4 * (2 ** _attempt)) + random.uniform(0, 0.2)
                    logger.warning(
                        f"[Binance REST] 未知异常 {method} {path}: {_err_detail}，"
                        f"第 {_attempt + 1}/{_max_retries} 次重试，等待 {_delay:.2f}s")
                    await asyncio.sleep(_delay)
                    continue
                logger.error(f"[Binance REST] 未知异常 {method} {path}: {_err_detail}")
                return None

        return None

    # ============ 交易所信息 ============

    async def get_exchange_info(self) -> Optional[dict]:
        """获取交易所信息 (exchangeInfo)"""
        return await self._rest_request("GET", "/fapi/v1/exchangeInfo")

    async def get_futures_contracts(self) -> List[dict]:
        """获取所有 BTC USDT-M 期货合约信息

        Returns:
            包含 BTCUSDT 和 BTCUSDT_YYMMDD 格式合约的列表
        """
        info = await self.get_exchange_info()
        if not info or "symbols" not in info:
            logger.error("[Binance WS] 获取 exchangeInfo 失败")
            return []

        btc_contracts = []
        for sym in info["symbols"]:
            symbol = sym.get("symbol", "")
            if symbol.startswith("BTCUSDT") and sym.get("status") == "TRADING":
                btc_contracts.append(sym)

        logger.info(f"[Binance WS] 找到 {len(btc_contracts)} 个 BTC USDT-M 合约")
        return btc_contracts

    # ============ Listen Key 管理 ============

    async def _create_listen_key(self) -> Optional[str]:
        """创建用户数据流 listenKey"""
        result = await self._rest_request("POST", "/fapi/v1/listenKey")
        if result and "listenKey" in result:
            self._listen_key = result["listenKey"]
            logger.info(f"[Binance WS] 创建 listenKey 成功: {self._listen_key[:20]}...")
            return self._listen_key
        logger.error(f"[Binance WS] 创建 listenKey 失败: {result}")
        return None

    async def _periodic_server_time_sync(self):
        """🌟 F/A5: 定期重同步 Binance serverTime, 抗本地时钟慢速漂移 (VM / NTP 异常)"""
        while self._running:
            try:
                # 首次已由 start() 同步过, 这里按间隔重同步
                _interval = getattr(self.auth, '_server_time_resync_interval', 3600)
                await asyncio.sleep(_interval)
                await self.auth.sync_server_time()
            except asyncio.CancelledError:
                break
            except Exception as _e:
                logger.info(f"[Binance WS] serverTime 重同步异常 (可忽略): {_e}")
                await asyncio.sleep(60)  # 异常时 1 分钟后重试, 避免 busy loop

    async def _keepalive_listen_key(self):
        """定期续期 listenKey (每 30 分钟)"""
        while self._running:
            try:
                await asyncio.sleep(30 * 60)  # 30 分钟
                if self._listen_key:
                    result = await self._rest_request(
                        "PUT", "/fapi/v1/listenKey", {"listenKey": self._listen_key})
                    # 🌟 P1-C 修复: 区分成功响应和错误响应
                    # Binance PUT listenKey 成功时返回 {} (空dict，无 code 字段)
                    # 失败时返回 {"code": -1125, "msg": "..."} 等带 code 的 dict
                    # 旧逻辑 `result is not None` 会把错误响应也当成功
                    if result is not None and not (isinstance(result, dict) and result.get('code')):
                        logger.info("[Binance WS] listenKey 续期成功")
                    else:
                        _err = result.get('msg', result) if isinstance(result, dict) else result
                        logger.warning(f"[Binance WS] listenKey 续期失败 ({_err})，触发用户数据流重连")
                        await self._reconnect_user_data(refresh_listen_key=True)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Binance WS] listenKey 续期异常: {e}")

    # ============ 市场数据 WebSocket ============

    async def connect_public_market(self, symbols: List[str]):
        """连接 Public 市场数据 WebSocket：depth10。"""
        self._subscribed_symbols = symbols

        streams = []
        for symbol in symbols:
            s = symbol.lower()
            streams.append(f"{s}@depth10@100ms")

        stream_path = "/".join(streams)
        ws_url = self.auth.build_public_stream_url(stream_path)

        logger.info(f"[Binance WS] 连接 Public 市场数据: {len(symbols)} 个 symbol, {len(streams)} 个流")
        logger.info(f"[Binance WS] Public WS URL: {ws_url}")

        try:
            self._public_ws = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=10 * 1024 * 1024,  # 10 MB
            )
            self._connected_public_market.set()
            self._refresh_market_connected_state()
            logger.info("[Binance WS] Public 市场数据 WebSocket 已连接")
        except Exception as e:
            self._connected_public_market.clear()
            self._refresh_market_connected_state()
            logger.error(f"[Binance WS] Public 市场数据连接失败: {e}")
            raise

    async def connect_regular_market(self, symbols: List[str]):
        """连接 Market 市场数据 WebSocket：markPrice + aggTrade。"""
        self._subscribed_symbols = symbols

        streams = []
        for symbol in symbols:
            s = symbol.lower()
            streams.append(f"{s}@markPrice@1s")
            streams.append(f"{s}@aggTrade")

        stream_path = "/".join(streams)
        ws_url = self.auth.build_market_stream_url(stream_path)

        logger.info(f"[Binance WS] 连接 Market 市场数据: {len(symbols)} 个 symbol, {len(streams)} 个流")
        logger.info(f"[Binance WS] Market WS URL: {ws_url}")

        try:
            self._regular_market_ws = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=10 * 1024 * 1024,
            )
            self._connected_regular_market.set()
            self._refresh_market_connected_state()
            logger.info("[Binance WS] Market 市场数据 WebSocket 已连接")
        except Exception as e:
            self._connected_regular_market.clear()
            self._refresh_market_connected_state()
            logger.error(f"[Binance WS] Market 市场数据连接失败: {e}")
            raise

    async def connect_market(self, symbols: List[str]):
        """兼容入口：同时连接 Public depth 与 Market mark/last 两路。"""
        await asyncio.gather(
            self.connect_public_market(symbols),
            self.connect_regular_market(symbols),
        )

    async def connect_user_data(self, force_new_key: bool = False):
        """连接用户数据 WebSocket (使用 listenKey)"""
        listen_key = None
        if not force_new_key and self._listen_key:
            listen_key = self._listen_key
        if not listen_key:
            listen_key = await self._create_listen_key()
        if not listen_key:
            logger.error("[Binance WS] 无法获取 listenKey，用户数据流不可用")
            return

        ws_url = f"{self.auth.ws_base}/ws/{listen_key}"

        try:
            self._user_ws = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            )
            self._connected_user.set()
            self.user_data_ready_at = time.time()
            logger.info("[Binance WS] 用户数据 WebSocket 已连接")
        except Exception as e:
            logger.error(f"[Binance WS] 用户数据连接失败: {e}")
            raise

    # ============ 消息处理 ============

    def _handle_depth(self, data: dict):
        """处理深度行情更新 (depth10)"""
        # Combined stream 格式: {"stream": "btcusdt@depth10@100ms", "data": {...}}
        stream = data.get("stream", "")
        payload = data.get("data", data)

        # 从 stream 名中解析 symbol
        symbol_lower = stream.split("@")[0] if stream else ""
        symbol = symbol_lower.upper()

        if not symbol:
            # 尝试从 payload 中的 s 字段获取
            symbol = payload.get("s", "")

        bids = [[Decimal(str(p)), Decimal(str(q))] for p, q in payload.get("b", payload.get("bids", []))]
        asks = [[Decimal(str(p)), Decimal(str(q))] for p, q in payload.get("a", payload.get("asks", []))]

        if symbol not in self.order_books:
            self.order_books[symbol] = BinanceOrderBook(symbol=symbol)

        ob = self.order_books[symbol]
        ob.bids = bids
        ob.asks = asks
        ob.update_time = time.time()
        _seen_key = ("depth", symbol)
        if symbol and _seen_key not in self._first_market_stream_seen:
            self._first_market_stream_seen.add(_seen_key)
            logger.info(f"[Binance WS] Public depth 首包: {symbol}")

    def _handle_mark_price(self, data: dict):
        """处理标记价格更新"""
        payload = data.get("data", data)
        symbol = payload.get("s", "")
        mark_price_str = payload.get("p", "0")
        funding_rate_str = payload.get("r", "0")

        if symbol:
            self.mark_prices[symbol] = Decimal(str(mark_price_str))
            self.mark_price_update_times[symbol] = time.time()
            if funding_rate_str and funding_rate_str != "":
                self.funding_rates[symbol] = Decimal(str(funding_rate_str))
            _seen_key = ("markPrice", symbol)
            if _seen_key not in self._first_market_stream_seen:
                self._first_market_stream_seen.add(_seen_key)
                logger.info(f"[Binance WS] Market markPrice 首包: {symbol}")

    def _handle_agg_trade(self, data: dict):
        """处理逐笔归集成交"""
        payload = data.get("data", data)
        symbol = payload.get("s", "")
        price_str = payload.get("p", "0")

        if symbol:
            self.last_prices[symbol] = Decimal(str(price_str))
            self.last_price_update_times[symbol] = time.time()
            _seen_key = ("aggTrade", symbol)
            if _seen_key not in self._first_market_stream_seen:
                self._first_market_stream_seen.add(_seen_key)
                logger.info(f"[Binance WS] Market aggTrade 首包: {symbol}")

    def _handle_order_update(self, data: dict):
        """处理订单更新 (ORDER_TRADE_UPDATE)"""
        order_info = data.get("o", {})
        update = {
            "symbol": order_info.get("s", ""),
            "order_id": order_info.get("i", 0),
            "client_order_id": order_info.get("c", ""),
            "side": order_info.get("S", ""),
            "order_type": order_info.get("o", ""),
            "status": order_info.get("X", ""),
            "price": Decimal(str(order_info.get("p", "0"))),
            "avg_price": Decimal(str(order_info.get("ap", "0"))),
            "orig_qty": Decimal(str(order_info.get("q", "0"))),
            "filled_qty": Decimal(str(order_info.get("z", "0"))),
            "realized_pnl": Decimal(str(order_info.get("rp", "0"))),
            "commission": Decimal(str(order_info.get("n", "0"))),
            "commission_asset": order_info.get("N", ""),
            "time": order_info.get("T", 0),
            "reduce_only": order_info.get("R", False),
            "position_side": order_info.get("ps", "BOTH"),
        }
        # 🌟 均价缓存: ORDER_TRADE_UPDATE 的 ap 字段通常比 REST 响应更早拿到真实均价
        # (测试网 MARKET/RESULT 响应常返回 avgPrice=0)，供 _backfill_avg_price 零成本回填
        try:
            if update["avg_price"] > 0 and update["order_id"]:
                self.order_avg_prices[str(update["order_id"])] = update["avg_price"]
                if len(self.order_avg_prices) > 500:
                    for _stale_oid in list(self.order_avg_prices.keys())[:250]:
                        self.order_avg_prices.pop(_stale_oid, None)
        except Exception:
            pass

        try:
            self._order_updates.put_nowait(update)
        except asyncio.QueueFull:
            logger.warning("[Binance WS] 订单更新队列已满，丢弃旧消息")
            try:
                self._order_updates.get_nowait()
                self._order_updates.put_nowait(update)
            except Exception:
                pass

        logger.debug(f"[Binance WS] 订单更新: {update['symbol']} {update['side']} "
                     f"状态={update['status']} 成交量={update['filled_qty']}/{update['orig_qty']}")

    def _handle_account_update(self, data: dict):
        """处理账户更新 (ACCOUNT_UPDATE)"""
        account_data = data.get("a", {})

        # 余额更新
        for balance_info in account_data.get("B", []):
            asset = balance_info.get("a", "")
            _wb = balance_info.get("wb", "0")  # wallet_balance 预留（当前仅用 cross_wallet）
            cross_wallet = Decimal(str(balance_info.get("cw", "0")))
            if asset:
                self.balances[asset] = cross_wallet
                logger.debug(f"[Binance WS] 余额更新: {asset} = {cross_wallet}")

        # 持仓更新
        for pos_info in account_data.get("P", []):
            symbol = pos_info.get("s", "")
            pos_amt = Decimal(str(pos_info.get("pa", "0")))
            entry_price = Decimal(str(pos_info.get("ep", "0")))
            unrealized_pnl = Decimal(str(pos_info.get("up", "0")))
            pos_side = str(pos_info.get("ps", "BOTH")).upper()
            mark_price = self.mark_prices.get(symbol, Decimal('0'))

            if symbol:
                if pos_side in ("LONG", "SHORT"):
                    key = (symbol, pos_side)
                    if pos_amt == 0:
                        self.positions_by_side.pop(key, None)
                        # 🌟 因果证据: 交易所主动推送的归零事件 (强平/ADL/外部平仓都会推)
                        self.position_zero_events[key] = time.time()
                        logger.debug(f"[Binance WS] 持仓清除: {symbol} {pos_side}")
                    else:
                        self.positions_by_side[key] = BinanceFuturesPosition(
                            symbol=symbol,
                            side=pos_side,
                            position_side=pos_side,
                            quantity=abs(pos_amt),
                            entry_price=entry_price,
                            unrealized_pnl=unrealized_pnl,
                            mark_price=mark_price,
                        )
                        logger.debug(f"[Binance WS] 持仓更新: {symbol} {pos_side} "
                                    f"数量={abs(pos_amt)} 入场价={entry_price}")
                    self._rebuild_net_position(symbol)
                else:
                    if pos_amt == 0:
                        self.positions.pop(symbol, None)
                        self.positions_by_side.pop((symbol, "LONG"), None)
                        self.positions_by_side.pop((symbol, "SHORT"), None)
                        _zero_ts = time.time()
                        for _zk in ((symbol, "LONG"), (symbol, "SHORT"), (symbol, "BOTH")):
                            self.position_zero_events[_zk] = _zero_ts
                        logger.debug(f"[Binance WS] 持仓清除: {symbol}")
                    else:
                        side = "LONG" if pos_amt > 0 else "SHORT"
                        self.positions[symbol] = BinanceFuturesPosition(
                            symbol=symbol,
                            side=side,
                            position_side="BOTH",
                            quantity=abs(pos_amt),
                            entry_price=entry_price,
                            unrealized_pnl=unrealized_pnl,
                            mark_price=mark_price,
                        )
                        logger.debug(f"[Binance WS] 持仓更新: {symbol} {side} "
                                    f"数量={abs(pos_amt)} 入场价={entry_price}")

    def _dispatch_message(self, raw_msg: str):
        """分发 WebSocket 消息到对应处理器"""
        try:
            data = orjson.loads(raw_msg)
        except Exception as e:
            logger.error(f"[Binance WS] JSON 解析失败: {e}")
            return

        # Combined stream 格式
        stream = data.get("stream", "")
        if stream:
            stream_l = stream.lower()
            if "depth" in stream_l:
                self._handle_depth(data)
            elif "markprice" in stream_l:
                self._handle_mark_price(data)
            elif "aggtrade" in stream_l:
                self._handle_agg_trade(data)
            return

        # 用户数据流格式
        event_type = data.get("e", "")
        if event_type == "ORDER_TRADE_UPDATE":
            self._handle_order_update(data)
        elif event_type == "ACCOUNT_UPDATE":
            self._handle_account_update(data)
        elif event_type == "listenKeyExpired":
            logger.warning("[Binance WS] listenKey 过期，将重新连接")
            asyncio.ensure_future(self._reconnect_user_data(refresh_listen_key=True))

    # ============ 监听循环 ============

    async def listen_public_market(self):
        """Public 市场数据监听循环 (depth, 带自动重连)"""
        while self._running:
            try:
                if self._public_ws is None:
                    await self.connect_public_market(self._subscribed_symbols)

                async for message in self._public_ws:
                    if not self._running:
                        break
                    self._dispatch_message(message)
                if self._running:
                    self._mark_market_disconnected("public", "Public stream ended")

            except websockets.ConnectionClosed as e:
                logger.warning(f"[Binance WS] Public 市场数据连接断开: {e}")
                self._mark_market_disconnected("public", f"Public ConnectionClosed:{e}")
            except Exception as e:
                logger.error(f"[Binance WS] Public 市场数据监听异常: {e}")
                self._mark_market_disconnected("public", f"Public {type(e).__name__}:{repr(e)}")

            if self._running:
                logger.info("[Binance WS] 5 秒后重连 Public 市场数据...")
                await asyncio.sleep(5)

    async def listen_regular_market(self):
        """Market 市场数据监听循环 (markPrice/aggTrade, 带自动重连)"""
        while self._running:
            try:
                if self._regular_market_ws is None:
                    await self.connect_regular_market(self._subscribed_symbols)

                async for message in self._regular_market_ws:
                    if not self._running:
                        break
                    self._dispatch_message(message)
                if self._running:
                    self._mark_market_disconnected("market", "Market stream ended")

            except websockets.ConnectionClosed as e:
                logger.warning(f"[Binance WS] Market 市场数据连接断开: {e}")
                self._mark_market_disconnected("market", f"Market ConnectionClosed:{e}")
            except Exception as e:
                logger.error(f"[Binance WS] Market 市场数据监听异常: {e}")
                self._mark_market_disconnected("market", f"Market {type(e).__name__}:{repr(e)}")

            if self._running:
                logger.info("[Binance WS] 5 秒后重连 Market 市场数据...")
                await asyncio.sleep(5)

    async def listen_market(self):
        """兼容入口：同时监听 Public 与 Market 两路市场数据。"""
        await asyncio.gather(
            self.listen_public_market(),
            self.listen_regular_market(),
        )

    async def listen_user_data(self):
        """用户数据监听循环 (带自动重连)"""
        while self._running:
            try:
                if self._user_ws is None:
                    await self.connect_user_data()

                if self._user_ws is None:
                    # 连接失败，等待重试
                    await asyncio.sleep(5)
                    continue

                async for message in self._user_ws:
                    if not self._running:
                        break
                    self._dispatch_message(message)

            except websockets.ConnectionClosed as e:
                logger.warning(f"[Binance WS] 用户数据连接断开: {e}")
                self._user_ws = None
                self._connected_user.clear()
            except Exception as e:
                logger.error(f"[Binance WS] 用户数据监听异常: {e}")
                self._user_ws = None
                self._connected_user.clear()

            if self._running:
                logger.info("[Binance WS] 5 秒后重连用户数据...")
                await asyncio.sleep(5)

    async def _reconnect_user_data(self, refresh_listen_key: bool = False):
        """重连用户数据流"""
        async with self._user_reconnect_lock:
            try:
                if self._user_ws:
                    await self._user_ws.close()
            except Exception:
                pass
            self._user_ws = None
            self._connected_user.clear()
            if refresh_listen_key:
                self._listen_key = None
            if not self._running:
                return
            try:
                await self.connect_user_data(force_new_key=refresh_listen_key)
            except Exception as e:
                logger.warning(f"[Binance WS] 用户数据重连失败，将由监听循环继续重试: {e}")

    # ============ REST 查询辅助 ============

    async def get_position_risk(self, symbol: str) -> Optional[dict]:
        """查询指定 symbol 的持仓风险 (REST 后备)"""
        params = {"symbol": symbol}
        result = await self._rest_request("GET", "/fapi/v2/positionRisk", params, signed=True)
        if result and isinstance(result, list):
            # 🌟 RISK-1 修复: 成功读取即恢复持仓快照可信标记 (同 get_position_risk_all)
            # API-R2-03: 初始快照循环内跳过 (可信与否由 start() 段尾按全量结果判定)
            if not self.position_snapshot_ok and not self._initial_snapshot_in_progress:
                self.position_snapshot_ok = True
                self.position_snapshot_at = time.time()
                logger.info("[Binance WS] positionRisk 查询成功, 持仓快照可信标记已恢复 (预热重新计时)")
            candidates = [pos for pos in result if pos.get("symbol") == symbol]
            if not candidates:
                return None
            for pos in candidates:
                try:
                    if Decimal(str(pos.get("positionAmt", "0"))) != 0:
                        return pos
                except Exception:
                    continue
            for pos in candidates:
                if str(pos.get("positionSide", "BOTH")).upper() == "BOTH":
                    return pos
            return candidates[0]
        return None

    async def get_position_risk_all(self, symbol: str) -> Optional[List[dict]]:
        """查询指定 symbol 的全部持仓风险记录（双向模式返回 LONG/SHORT 两条）

        返回:
            list: 成功请求（可为空列表，表示确认该 symbol 无仓位）
            None: 请求失败或返回错误，仓位状态未知
        """
        params = {"symbol": symbol}
        result = await self._rest_request("GET", "/fapi/v2/positionRisk", params, signed=True)
        if result is None:
            return None
        if isinstance(result, dict) and result.get("code") is not None:
            logger.warning(f"[Binance WS] positionRisk 查询失败: {result}")
            return None
        rows: List[dict] = []
        if result and isinstance(result, list):
            for pos in result:
                if pos.get("symbol") == symbol:
                    rows.append(pos)
        elif result and isinstance(result, dict) and result.get("symbol") == symbol:
            rows.append(result)
        # 🌟 RISK-1 修复: 启动快照失败后, 任意一次成功的 positionRisk 读取即恢复可信标记
        # (一次性恢复 + 预热重新计时; ok=True 后不再刷新, 防止预热时钟被每次读取重置)
        # API-R2-03: 初始快照循环内跳过 (可信与否由 start() 段尾按全量结果判定)
        if not self.position_snapshot_ok and not self._initial_snapshot_in_progress:
            self.position_snapshot_ok = True
            self.position_snapshot_at = time.time()
            logger.info("[Binance WS] positionRisk 查询成功, 持仓快照可信标记已恢复 (预热重新计时)")
        return rows

    async def get_account_info(self) -> Optional[dict]:
        """查询账户信息 (REST 后备)"""
        return await self._rest_request("GET", "/fapi/v2/account", signed=True)

    async def get_position_mode(self) -> Optional[bool]:
        """查询持仓模式：True=双向(Hedge), False=单向(One-way)"""
        result = await self._rest_request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if isinstance(result, dict) and "dualSidePosition" in result:
            val = result.get("dualSidePosition")
            if isinstance(val, bool):
                self.dual_side_mode = val
                return val
            if isinstance(val, str):
                self.dual_side_mode = val.lower() == "true"
                return self.dual_side_mode
        return None

    async def set_position_mode(self, enabled: bool) -> bool:
        """设置持仓模式：enabled=True 切换双向(Hedge)"""
        params = {"dualSidePosition": "true" if enabled else "false"}
        result = await self._rest_request("POST", "/fapi/v1/positionSide/dual", params, signed=True)
        if isinstance(result, dict) and result.get("code") in (200, "200"):
            self.dual_side_mode = enabled
            return True
        if isinstance(result, dict) and str(result.get("msg", "")).find("No need to change position side") >= 0:
            self.dual_side_mode = enabled
            return True
        if isinstance(result, dict) and result.get("code") == -4068:
            # 账户存在持仓时，Binance 不允许切换持仓模式
            logger.warning("[Binance WS] 设置持仓模式失败(-4068): 账户有持仓，无法在线切换。")
            return False
        logger.warning(f"[Binance WS] 设置持仓模式失败: {result}")
        return False

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置指定合约的杠杆倍数 (1-125, 但实际受账户可用余额限制)

        Args:
            symbol: 如 "BTCUSDT"
            leverage: 杠杆倍数, 建议实盘 3-5, 测试网可以更高

        Returns:
            True 成功, False 失败
        """
        params = {"symbol": symbol, "leverage": int(leverage)}
        result = await self._rest_request("POST", "/fapi/v1/leverage", params, signed=True)
        if isinstance(result, dict) and result.get("leverage") is not None:
            logger.info(f"[Binance WS] ✅ 杠杆已设置: {symbol} leverage={result.get('leverage')}x "
                        f"maxNotionalValue={result.get('maxNotionalValue')}")
            return True
        logger.warning(f"[Binance WS] 设置杠杆失败: {symbol} leverage={leverage} → {result}")
        return False

    async def set_margin_type(self, symbol: str, margin_type: str) -> bool:
        """设置保证金模式: "ISOLATED" (隔离) 或 "CROSSED" (全仓)

        Args:
            symbol: 如 "BTCUSDT"
            margin_type: "ISOLATED" 或 "CROSSED"

        Returns:
            True 成功 (或已是目标模式), False 失败
        """
        mt = str(margin_type).upper()
        if mt not in ("ISOLATED", "CROSSED"):
            logger.error(f"[Binance WS] 无效的 margin_type: {margin_type} (应为 ISOLATED 或 CROSSED)")
            return False
        params = {"symbol": symbol, "marginType": mt}
        result = await self._rest_request("POST", "/fapi/v1/marginType", params, signed=True)
        # 成功: {"code": 200, "msg": "success"}
        # 已是目标模式: {"code": -4046, "msg": "No need to change margin type."}
        if isinstance(result, dict):
            code = result.get("code")
            if code in (200, "200"):
                logger.info(f"[Binance WS] ✅ 保证金模式已设置: {symbol} → {mt}")
                return True
            if code == -4046:
                logger.info(f"[Binance WS] ✅ 保证金模式已是 {mt} (无需切换)")
                return True
            if code == -4048:
                # 账户存在持仓时不允许切换
                logger.warning(f"[Binance WS] 保证金模式切换失败(-4048): 账户有持仓, 无法切换")
                return False
        logger.warning(f"[Binance WS] 设置保证金模式失败: {symbol} → {mt} | result={result}")
        return False

    def _rebuild_net_position(self, symbol: str):
        """由 LONG/SHORT 分腿重建 net 持仓（兼容旧逻辑）"""
        long_pos = self.positions_by_side.get((symbol, "LONG"))
        short_pos = self.positions_by_side.get((symbol, "SHORT"))
        long_qty = long_pos.quantity if long_pos else Decimal('0')
        short_qty = short_pos.quantity if short_pos else Decimal('0')
        net = long_qty - short_qty
        if net > 0:
            self.positions[symbol] = BinanceFuturesPosition(
                symbol=symbol,
                side="LONG",
                position_side="BOTH",
                quantity=net,
                entry_price=long_pos.entry_price if long_pos else Decimal('0'),
                unrealized_pnl=(long_pos.unrealized_pnl if long_pos else Decimal('0')) - (
                    short_pos.unrealized_pnl if short_pos else Decimal('0')),
                mark_price=long_pos.mark_price if long_pos and long_pos.mark_price > 0 else (
                    short_pos.mark_price if short_pos else Decimal('0')),
            )
        elif net < 0:
            self.positions[symbol] = BinanceFuturesPosition(
                symbol=symbol,
                side="SHORT",
                position_side="BOTH",
                quantity=abs(net),
                entry_price=short_pos.entry_price if short_pos else Decimal('0'),
                unrealized_pnl=(long_pos.unrealized_pnl if long_pos else Decimal('0')) - (
                    short_pos.unrealized_pnl if short_pos else Decimal('0')),
                mark_price=short_pos.mark_price if short_pos and short_pos.mark_price > 0 else (
                    long_pos.mark_price if long_pos else Decimal('0')),
            )
        else:
            self.positions.pop(symbol, None)

    # ============ 生命周期 ============

    async def start(self, symbols: List[str] = None):
        """启动客户端：连接 WS、启动监听任务

        Args:
            symbols: 要订阅的 symbol 列表，默认为 ["BTCUSDT"]
        """
        if symbols is None:
            symbols = ["BTCUSDT"]

        self._running = True
        self._subscribed_symbols = symbols

        logger.info(f"[Binance WS] 启动中... 订阅 symbols: {symbols}")

        # 🌟 F/A5 修复: 启动前同步 serverTime, 避免本地时钟漂移导致所有签名请求 -1022
        # 这一步必须在任何签名 REST (listenKey/positionSide/leverage/order) 之前
        try:
            _sync_ok = await self.auth.sync_server_time()
            if not _sync_ok:
                logger.warning("[Binance WS] serverTime 首次同步失败, 后续签名请求可能受本地时钟影响")
        except Exception as _e:
            logger.warning(f"[Binance WS] serverTime 同步异常 (非致命, 继续启动): {_e}")

        # 启动所有异步任务
        self._tasks = [
            asyncio.create_task(self.listen_public_market(), name="binance_public_market"),
            asyncio.create_task(self.listen_regular_market(), name="binance_regular_market"),
            asyncio.create_task(self.listen_user_data(), name="binance_user_data"),
            asyncio.create_task(self._keepalive_listen_key(), name="binance_keepalive"),
            # 🌟 F/A5: 定期重同步 serverTime, 抗慢速漂移
            asyncio.create_task(self._periodic_server_time_sync(), name="binance_time_sync"),
        ]

        # 等待连接建立 (最多 15 秒)
        try:
            await asyncio.wait_for(self._connected_market.wait(), timeout=15)
            logger.info("[Binance WS] 市场数据连接就绪")
        except asyncio.TimeoutError:
            logger.warning("[Binance WS] 市场数据连接超时 (15s)，继续启动...")

        try:
            await asyncio.wait_for(self._connected_user.wait(), timeout=15)
            logger.info("[Binance WS] 用户数据连接就绪")
        except asyncio.TimeoutError:
            logger.warning("[Binance WS] 用户数据连接超时 (15s)，继续启动...")

        # 查询当前持仓模式
        try:
            _mode = await self.get_position_mode()
            if _mode is not None:
                logger.info(f"[Binance WS] 当前持仓模式: {'Hedge(双向)' if _mode else 'One-way(单向)'}")
        except Exception as e:
            logger.warning(f"[Binance WS] 持仓模式查询失败: {e}")

        # ===== 通过 REST 拉取初始持仓快照（WS 只推增量，不含历史持仓）=====
        # 🌟 2026-06-11 修复: 记录快照成功/失败状态, 供上层判断"空仓读数"可信度
        self.position_snapshot_ok = False
        self.position_snapshot_at = 0.0
        self._initial_snapshot_in_progress = True
        _snapshot_all_ok = True
        try:
            for sym in symbols:
                risks = await self.get_position_risk_all(sym)
                if risks is None:
                    _snapshot_all_ok = False
                    logger.warning(f"[Binance WS] 初始持仓查询失败（状态未知）: {sym}，等待后续WS/REST同步")
                    continue
                for risk in risks:
                    pos_amt = Decimal(str(risk.get("positionAmt", "0")))
                    pos_side = str(risk.get("positionSide", "BOTH")).upper()
                    if pos_amt == 0:
                        continue
                    if pos_side in ("LONG", "SHORT"):
                        self.positions_by_side[(sym, pos_side)] = BinanceFuturesPosition(
                            symbol=sym,
                            side=pos_side,
                            position_side=pos_side,
                            quantity=abs(pos_amt),
                            entry_price=Decimal(str(risk.get("entryPrice", "0"))),
                            unrealized_pnl=Decimal(str(risk.get("unRealizedProfit", "0"))),
                            mark_price=Decimal(str(risk.get("markPrice", "0"))),
                        )
                        logger.info(f"[Binance WS] 初始持仓加载: {sym} {pos_side} 数量={abs(pos_amt)} 入场价={risk.get('entryPrice')}")
                    else:
                        side = "LONG" if pos_amt > 0 else "SHORT"
                        self.positions[sym] = BinanceFuturesPosition(
                            symbol=sym,
                            side=side,
                            position_side="BOTH",
                            quantity=abs(pos_amt),
                            entry_price=Decimal(str(risk.get("entryPrice", "0"))),
                            unrealized_pnl=Decimal(str(risk.get("unRealizedProfit", "0"))),
                            mark_price=Decimal(str(risk.get("markPrice", "0"))),
                        )
                        logger.info(f"[Binance WS] 初始持仓加载: {sym} {side} 数量={abs(pos_amt)} 入场价={risk.get('entryPrice')}")
                if self.positions_by_side.get((sym, "LONG")) or self.positions_by_side.get((sym, "SHORT")):
                    self._rebuild_net_position(sym)
            self._initial_snapshot_in_progress = False
            if _snapshot_all_ok:
                self.position_snapshot_ok = True
                self.position_snapshot_at = time.time()
            _has_any_pos = bool(self.positions) or any(
                p.quantity > 0 for p in self.positions_by_side.values())
            if _has_any_pos:
                logger.info(
                    f"[Binance WS] 初始持仓加载完成: net={len(self.positions)}, "
                    f"分腿={len(self.positions_by_side)}")
            else:
                # ⚠️ 空仓快照可能是测试网/API 状态不一致的假空 (2026-06-11 事故),
                # 引擎侧依据 position_snapshot_ok + 因果证据决定是否采信
                logger.info("[Binance WS] 当前无持仓 (快照成功=%s)" % _snapshot_all_ok)
        except Exception as e:
            logger.warning(f"[Binance WS] 初始持仓加载失败（不影响运行）: {e}")
        finally:
            # 异常路径也必须复位, 否则运行期恢复通道 (RISK-1a) 被永久关闭
            self._initial_snapshot_in_progress = False

    async def close(self):
        """清理关闭：断开 WS、取消任务、关闭 HTTP 会话"""
        logger.info("[Binance WS] 正在关闭...")
        self._running = False

        # 取消所有任务
        for task in self._tasks:
            task.cancel()

        # 等待任务结束
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # 关闭 WebSocket 连接
        if self._public_ws:
            try:
                await self._public_ws.close()
            except Exception:
                pass
            self._public_ws = None

        if self._regular_market_ws:
            try:
                await self._regular_market_ws.close()
            except Exception:
                pass
            self._regular_market_ws = None

        if self._market_ws:
            try:
                await self._market_ws.close()
            except Exception:
                pass
            self._market_ws = None

        if self._user_ws:
            try:
                await self._user_ws.close()
            except Exception:
                pass
            self._user_ws = None

        # 关闭 HTTP 会话
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        self._connected_market.clear()
        self._connected_public_market.clear()
        self._connected_regular_market.clear()
        self._connected_user.clear()
        self.market_connected_at = 0.0

        logger.info("[Binance WS] 已关闭")


# ================= Binance 期货订单执行器 =================
class BinanceFuturesExecutor:
    """Binance 期货订单执行器（对冲腿）

    功能：
    - 合约信息加载 (tick_size, step_size, min_qty)
    - 市价单 / 限价 IOC 单
    - 持仓平仓
    - 对冲下单主入口 (支持市价 + IOC 回退)
    """

    def __init__(self, ws_client: BinanceFuturesWSClient):
        self.ws_client = ws_client
        self.auth = ws_client.auth

        # 合约精度信息：{symbol: {"tick_size": Decimal, "step_size": Decimal, "min_qty": Decimal}}
        self.contract_info: Dict[str, dict] = {}
        self._last_order_error: Optional[int] = None  # 最近一次下单失败的错误码（如 -2019 保证金不足）

        logger.info("[Binance执行器] 初始化完成")

    async def load_contract_info(self):
        """从 exchangeInfo 加载合约精度信息 (tick_size, step_size, min_qty)"""
        info = await self.ws_client.get_exchange_info()
        if not info or "symbols" not in info:
            logger.error("[Binance执行器] 无法加载 exchangeInfo")
            return

        for sym in info["symbols"]:
            symbol = sym.get("symbol", "")
            if not symbol.startswith("BTCUSDT"):
                continue

            tick_size = Decimal('0.01')  # 默认值
            step_size = Decimal('0.001')
            min_qty = Decimal('0.001')

            for f in sym.get("filters", []):
                filter_type = f.get("filterType", "")
                if filter_type == "PRICE_FILTER":
                    tick_size = Decimal(str(f.get("tickSize", "0.01")))
                elif filter_type == "LOT_SIZE":
                    step_size = Decimal(str(f.get("stepSize", "0.001")))
                    min_qty = Decimal(str(f.get("minQty", "0.001")))
                elif filter_type == "MARKET_LOT_SIZE":
                    # 市价单的额外限制
                    market_step = Decimal(str(f.get("stepSize", "0")))
                    if market_step > 0:
                        step_size = max(step_size, market_step)

            self.contract_info[symbol] = {
                "tick_size": tick_size,
                "step_size": step_size,
                "min_qty": min_qty,
            }

        logger.info(f"[Binance执行器] 加载了 {len(self.contract_info)} 个合约精度信息")
        for symbol, info_dict in self.contract_info.items():
            logger.info(f"  {symbol}: tick={info_dict['tick_size']}, "
                          f"step={info_dict['step_size']}, min={info_dict['min_qty']}")

    @staticmethod
    def _generate_client_order_id(prefix: str = "arb") -> str:
        """🌟 P1-1: 生成 client_order_id (防重复下单)
        格式: arb_<时间戳ms>_<4位随机>
        Binance 要求: 最多 36 字符, 字母数字+-_
        """
        import time as _t
        import random as _r
        return f"{prefix}_{int(_t.time() * 1000)}_{_r.randint(1000, 9999)}"

    def _validate_recovered_order(self, recovered: dict, expected_symbol: str,
                                    expected_side: str, expected_qty: Decimal) -> bool:
        """🌟 P2 修复: 校验 -5022 查单返回的订单是否真的是本次请求
        若 client_order_id 发生碰撞, Binance 可能返回另一笔订单; 盲目采纳会导致
        对冲方向/数量误判。本函数对比 symbol/side/origQty, 任一不符即判为碰撞。

        Args:
            recovered: _query_order_by_client_oid 返回的订单 dict
            expected_symbol / expected_side / expected_qty: 本次请求的参数
        Returns:
            True: 匹配, 可安全采纳; False: 碰撞或数据异常, 上层应按失败处理
        """
        try:
            if not isinstance(recovered, dict):
                return False
            if recovered.get('_uncertain_filled'):
                # 哨兵本身不用校验 (走对账流程)
                return True
            _sym = str(recovered.get('symbol', ''))
            _side = str(recovered.get('side', '')).upper()
            _orig = recovered.get('origQty') or recovered.get('executedQty') or '0'
            _orig_qty = Decimal(str(_orig))
            _exp_side = str(expected_side).upper()
            # 对比 symbol
            if _sym and expected_symbol and _sym != expected_symbol:
                logger.error(
                    f"[Binance执行器] 🚨 -5022 查单 symbol 不匹配! "
                    f"expected={expected_symbol} got={_sym} → 判定为 client_oid 碰撞")
                return False
            # 对比 side
            if _side and _exp_side and _side != _exp_side:
                logger.error(
                    f"[Binance执行器] 🚨 -5022 查单 side 不匹配! "
                    f"expected={_exp_side} got={_side} → 判定为 client_oid 碰撞")
                return False
            # 对比数量 (容差 1% 或 0.0001, 取大; 处理 Binance step_size 舍入)
            _tol = max(expected_qty * Decimal('0.01'), Decimal('0.0001'))
            if abs(_orig_qty - expected_qty) > _tol:
                logger.error(
                    f"[Binance执行器] 🚨 -5022 查单数量偏差过大! "
                    f"expected={expected_qty} got={_orig_qty} tol={_tol} → 判定为 client_oid 碰撞")
                return False
            return True
        except Exception as _e:
            logger.error(f"[Binance执行器] _validate_recovered_order 异常: {_e}, 保守判失败")
            return False

    async def _reconcile_position_after_uncertain(self, symbol: str, client_oid: str,
                                                    expected_side: str, expected_qty: Decimal) -> Optional[dict]:
        """🌟 E/A4 修复: -5022 查单多次失败时的仓位对账兜底

        逻辑: 调 REST /fapi/v2/positionRisk 对比本地 positions_by_side 快照,
        - 若实际仓位比期望多 (|delta| ≈ expected_qty), 认为下单其实成交了, 构造一个伪成功 dict 返回
          方向匹配: BUY → LONG 增多 | SELL → SHORT 增多 (双向模式) / net 变化 (单向模式)
        - 若实际仓位与本地一致, 认为下单确实失败, 返回 None (上层可重试)
        - 若无法判定 (REST 失败 或 中间状态), 阻塞 3 秒再查一次 (Binance 撮合可能延迟)

        这是最后一道防线, 不应频繁触发。每次触发都告警。
        """
        try:
            # 查询实际账户仓位
            params = {"symbol": symbol}
            pos_risk = await self.ws_client._rest_request(
                "GET", "/fapi/v2/positionRisk", params, signed=True)
            if not isinstance(pos_risk, list):
                logger.error(f"[对账] positionRisk 返回非列表: {pos_risk}, 判定为失败")
                return None

            # 提取 BOTH / LONG / SHORT 的 positionAmt
            _pos_amts = {}
            for _p in pos_risk:
                if _p.get('symbol') == symbol:
                    _side = _p.get('positionSide', 'BOTH')
                    _pos_amts[_side] = Decimal(str(_p.get('positionAmt', '0')))

            # 本地快照 (🌟 修正: BinanceFuturesExecutor 没有 positions 属性, 必须走 self.ws_client)
            # 原错误: self.positions.get() / self.positions_by_side.get() 在 Executor 上永远是 AttributeError
            #   旧 hasattr 检查让 _local_long/_local_short 永远为 None, 未来扩展会踩坑
            _local_both = self.ws_client.positions.get(symbol)
            _local_long = self.ws_client.positions_by_side.get((symbol, 'LONG'))
            _local_short = self.ws_client.positions_by_side.get((symbol, 'SHORT'))

            _expected_delta = expected_qty if expected_side.upper() == 'BUY' else -expected_qty

            # 对比 (主要用 BOTH 或 净仓位)
            _actual_both = _pos_amts.get('BOTH', Decimal('0'))
            _local_qty = _local_both.quantity if _local_both else Decimal('0')
            _delta = _actual_both - _local_qty

            logger.warning(
                f"[对账] symbol={symbol} expected_delta={_expected_delta} "
                f"actual_both={_actual_both} local_both={_local_qty} delta={_delta}")

            # 判定: 差额方向与符号匹配, |差额| ≈ 期望数量 (容差 1%)
            _tol = max(expected_qty * Decimal('0.01'), Decimal('0.00001'))
            if (abs(_delta - _expected_delta) <= _tol):
                logger.warning(
                    f"[对账] ✅ 推断下单已成交 (实际仓位与预期 delta 吻合), "
                    f"构造伪成功返回")
                # 🌟 A4-2: 伪成功 dict 的 avgPrice='0' 会污染上层加权均价 (开仓追单分母/
                # 平仓 VWAP), 用标记价/盘口估算并打 mark_fallback 标记, 让既有消费链识别为估算价
                _recon_avg = Decimal('0')
                _mk = self.mark_prices.get(symbol, Decimal('0'))
                if _mk and _mk > 0:
                    _recon_avg = _mk
                else:
                    _ob = self.order_books.get(symbol)
                    if _ob and _ob.mid_price and _ob.mid_price > 0:
                        _recon_avg = Decimal(str(_ob.mid_price))
                _recon = {
                    "_reconciled_uncertain": True,
                    "clientOrderId": client_oid,
                    "symbol": symbol,
                    "status": "FILLED",
                    "executedQty": str(expected_qty),
                    "origQty": str(expected_qty),
                    "avgPrice": str(_recon_avg),
                    "side": expected_side.upper(),
                }
                if _recon_avg > 0:
                    _recon["avgPriceSource"] = "mark_fallback"
                else:
                    logger.warning(f"[对账] 伪成功 dict 无标记价/盘口可估, avgPrice 保留 0")
                return _recon
            elif abs(_delta) <= _tol:
                logger.warning(
                    f"[对账] ⭕ 实际仓位与本地吻合, 判定下单真失败, 返回 None 让上层重试")
                return None
            else:
                logger.error(
                    f"[对账] ⚠️ 无法判定 (delta={_delta} 与预期 {_expected_delta} 不匹配), "
                    f"保守返回 None, 告警人工介入")
                return None
        except Exception as _e:
            logger.error(f"[对账] 异常: {_e}, 返回 None")
            return None

    async def _query_order_by_client_oid(self, symbol: str, client_oid: str,
                                            max_retries: int = 3) -> Optional[dict]:
        """🌟 P0-2 + E/A4 修复: 通过 origClientOrderId 查询订单状态 (用于 -5022 Duplicate 恢复)

        当下单遇到 -5022 (Duplicate order id) 时, 说明 Binance 已接收过此订单.
        此时用 origClientOrderId 查询真实订单状态, 避免误判为失败导致裸腿.

        🌟 E/A4 增强: 首次查询失败时进行最多 max_retries 次指数退避重试。
        若全部失败, 返回 UNCERTAIN_FILLED 哨兵 dict 而非 None, 让上层路由到
        仓位对账路径 (比较 REST positionRisk 与本地期望) 而不是当失败重下单。

        Args:
            symbol: 合约符号
            client_oid: 之前提交的 client_order_id
            max_retries: 最大重试次数 (默认 3)

        Returns:
            - 成功: 订单字典 (含 orderId/status/executedQty/avgPrice 等)
            - 不确定成交: {"_uncertain_filled": True, "symbol": ..., "client_oid": ...,
                        "code": -5022, "msg": "query exhausted"}
            - 其他: None
        """
        params = {"symbol": symbol, "origClientOrderId": client_oid}
        _last_err = None
        for _attempt in range(1, max_retries + 1):
            try:
                result = await self.ws_client._rest_request("GET", "/fapi/v1/order", params, signed=True)
                if isinstance(result, dict) and result.get('orderId'):
                    logger.info(
                        f"[Binance执行器] ✅ 查单成功(client_oid={client_oid}, 第{_attempt}次): "
                        f"orderId={result.get('orderId')} status={result.get('status')} "
                        f"executedQty={result.get('executedQty')} avgPrice={result.get('avgPrice')}")
                    return result
                _last_err = result
                logger.warning(
                    f"[Binance执行器] ⚠️ 查单失败第 {_attempt}/{max_retries} 次 "
                    f"client_oid={client_oid} result={result}")
            except Exception as _e:
                _last_err = str(_e)
                logger.warning(f"[Binance执行器] 查单第 {_attempt} 次异常: {_e}")
            # 指数退避
            if _attempt < max_retries:
                await asyncio.sleep(0.4 * (2 ** _attempt))
        # 全部重试失败, 返回 UNCERTAIN_FILLED 哨兵, 让上层触发仓位对账
        logger.error(
            f"[Binance执行器] ❌ 查单 {max_retries} 次全部失败, 返回 UNCERTAIN_FILLED "
            f"(client_oid={client_oid}, last_err={_last_err})")
        return {
            "_uncertain_filled": True,
            "symbol": symbol,
            "client_oid": client_oid,
            "code": -5022,
            "msg": f"query exhausted after {max_retries} retries",
            "last_err": str(_last_err)[:200],
        }

    def _round_price(self, symbol: str, price: Decimal) -> Decimal:
        """将价格对齐到 tick_size"""
        info = self.contract_info.get(symbol)
        if not info:
            logger.warning(f"[Binance执行器] 未找到 {symbol} 的合约信息，使用原始价格")
            return price
        tick_size = info["tick_size"]
        if tick_size == 0:
            return price
        return (price / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size

    def _round_qty(self, symbol: str, qty: Decimal) -> Decimal:
        """将数量对齐到 step_size"""
        info = self.contract_info.get(symbol)
        if not info:
            logger.warning(f"[Binance执行器] 未找到 {symbol} 的合约信息，使用原始数量")
            return qty
        step_size = info["step_size"]
        if step_size == 0:
            return qty
        return (qty / step_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * step_size

    async def _backfill_avg_price(self, result: Optional[dict], symbol: str,
                                  max_requery: int = 1) -> Optional[dict]:
        """成交回报均价缺失时回填 (测试网 MARKET/RESULT 响应常返回 avgPrice=0)。

        回填链: cumQuote 推算 → WS ORDER_TRADE_UPDATE 缓存 → REST 查单 → 标记价兜底(打标)。
        标记价兜底会写 result['avgPriceSource']='mark_fallback'，PnL 路径须识别为估算价。
        纯只读操作，不下单；max_requery 控制 REST 重查次数 (紧急路径保持1次防拖慢分片)。
        """
        try:
            if not result:
                return result
            _status = str(result.get('status', '')).upper()
            _exec_qty = Decimal(str(result.get('executedQty', '0') or '0'))
            _avg = Decimal(str(result.get('avgPrice', '0') or '0'))
            # EXPIRED = IOC 部分成交后剩余被取消, executedQty>0 时同样需要回填均价
            if _status not in ('FILLED', 'PARTIALLY_FILLED', 'EXPIRED') or _exec_qty <= 0 or _avg > 0:
                return result
            _cq = Decimal(str(result.get('cumQuote', '0') or '0'))
            if _cq > 0:
                result['avgPrice'] = str(_cq / _exec_qty)
                result['avgPriceSource'] = 'cum_quote'
                return result
            _oid = str(result.get('orderId', '') or '')
            for _i in range(max(1, max_requery)):
                if _oid:
                    _cached = self.ws_client.order_avg_prices.get(_oid, Decimal('0'))
                    if _cached > 0:
                        result['avgPrice'] = str(_cached)
                        result['avgPriceSource'] = 'ws_cache'
                        return result
                await asyncio.sleep(0.25 * (_i + 1))
                if _oid:
                    try:
                        _q = await self.ws_client._rest_request(
                            "GET", "/fapi/v1/order",
                            {"symbol": symbol, "orderId": _oid}, signed=True)
                        if isinstance(_q, dict) and _q.get('code') is None:
                            _qa = Decimal(str(_q.get('avgPrice', '0') or '0'))
                            _qcq = Decimal(str(_q.get('cumQuote', '0') or '0'))
                            _qz = Decimal(str(_q.get('executedQty', '0') or '0'))
                            if _qa > 0:
                                result['avgPrice'] = str(_qa)
                                result['avgPriceSource'] = 'rest_requery'
                                return result
                            if _qcq > 0 and _qz > 0:
                                result['avgPrice'] = str(_qcq / _qz)
                                result['avgPriceSource'] = 'rest_cum_quote'
                                return result
                    except Exception:
                        pass
            _mark = self.ws_client.mark_prices.get(symbol, Decimal('0'))
            if _mark <= 0:
                _ob = self.ws_client.order_books.get(symbol)
                if _ob and _ob.mid_price and _ob.mid_price > 0:
                    _mark = Decimal(str(_ob.mid_price))
            if _mark > 0:
                result['avgPrice'] = str(_mark)
                result['avgPriceSource'] = 'mark_fallback'
                logger.warning(
                    f"[Binance执行器] {symbol} orderId={_oid} 均价回填失败, "
                    f"以标记价 {_mark} 估算并打标 (avgPriceSource=mark_fallback)")
            else:
                logger.warning(f"[Binance执行器] {symbol} orderId={_oid} 均价回填失败且无标记价可用")
            return result
        except Exception as _bf_err:
            logger.warning(f"[Binance执行器] 均价回填异常 (忽略, 保留原回报): {_bf_err}")
            return result

    async def place_market_order(self, symbol: str, side: str, quantity: Decimal,
                                  reduce_only: bool = False, position_side: Optional[str] = None) -> Optional[dict]:
        """下市价单

        Args:
            symbol: 合约 symbol
            side: "BUY" 或 "SELL"
            quantity: 下单数量
            reduce_only: 是否仅减仓

        Returns:
            订单回报 dict 或 None
        """
        qty = self._round_qty(symbol, quantity)

        # 检查最小下单量
        info = self.contract_info.get(symbol, {})
        min_qty = info.get("min_qty", Decimal('0.001'))
        if qty < min_qty:
            logger.warning(f"[Binance执行器] 下单数量 {qty} 小于最小值 {min_qty}，取消下单")
            return None

        # 🌟 P1-1: 生成 client_order_id 防重复下单 (网络超时重试时幂等)
        client_oid = self._generate_client_order_id("mkt")
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": str(qty),
            "newClientOrderId": client_oid,
            "newOrderRespType": "RESULT",  # 等待撮合完成后返回成交结果，避免返回 NEW 状态
        }
        _ps = (position_side or "").upper()
        if _ps in ("LONG", "SHORT", "BOTH"):
            params["positionSide"] = _ps
        # 双向持仓模式下，reduceOnly 常被交易所拒绝；依赖 positionSide + 精确数量平仓
        if reduce_only and _ps not in ("LONG", "SHORT"):
            params["reduceOnly"] = "true"

        logger.info(f"[Binance执行器] 市价单: {symbol} {side} 数量={qty} "
                    f"reduceOnly={reduce_only} positionSide={_ps or 'AUTO'} cid={client_oid}")

        result = await self.ws_client._rest_request("POST", "/fapi/v1/order", params, signed=True)
        if result and 'orderId' in result:
            # 🌟 2026-06-11 修复: 测试网常返回 FILLED+avgPrice=0, 回填真实均价防污染 PnL
            result = await self._backfill_avg_price(result, symbol)
            order_id = result.get("orderId", "")
            status = result.get("status", "")
            avg_price = result.get("avgPrice", "0")
            _avg_src = result.get("avgPriceSource", "")
            logger.info(f"[Binance执行器] 市价单结果: orderId={order_id} 状态={status} "
                        f"均价={avg_price}{f' (来源:{_avg_src})' if _avg_src else ''}")
            return result
        else:
            err_code = result.get('code', '') if result else ''
            err_msg = result.get('msg', '') if result else 'No response'
            # 🌟 P0-2: -5022 Duplicate order id 幂等恢复
            # 说明: Binance 已接收过这个 client_oid, 第一次请求可能已成功撮合但响应丢失
            # 重试 → 查询真实订单状态, 避免误判失败导致裸腿
            if err_code in (-5022, "-5022"):
                logger.warning(f"[Binance执行器] 检测到 -5022 Duplicate, 查询真实订单状态...")
                _query_result = await self._query_order_by_client_oid(symbol, client_oid)
                if _query_result:
                    # 🌟 E/A4: 若返回 UNCERTAIN_FILLED 哨兵, 进行仓位对账再决定
                    if _query_result.get('_uncertain_filled'):
                        logger.error(
                            f"[Binance执行器] 🚨 -5022 查单全部失败, 触发仓位对账 "
                            f"(symbol={symbol} client_oid={client_oid})")
                        # 🌟 codex 审核修复: 必须用舍入后 qty (Binance 实际接收的量), 不能用原始 quantity
                        # 原 bug: 传 quantity 会让对账 _expected_delta 与 Binance 实际仓位变动
                        #        相差一个 step_size, 容差边界场景可能误判为失败/碰撞;
                        #        且伪成功 dict 的 executedQty/origQty 会写回 quantity (偏差状态机)
                        _recon = await self._reconcile_position_after_uncertain(
                            symbol, client_oid, side, qty)
                        return _recon
                    # 🌟 P2 修复: 防 client_oid 碰撞 — 校验 symbol/side/qty 是否与本请求一致
                    # 🌟 codex 审核修复: 用 qty (与 Binance 实际记录的 origQty 同步), 不用 quantity
                    if not self._validate_recovered_order(_query_result, symbol, side, qty):
                        logger.error(
                            f"[Binance执行器] 🚨 -5022 查单结果校验失败 (疑似 client_oid 碰撞), "
                            f"判本次下单失败; 交给上层重试或对账")
                        return None
                    # 🌟 API-R2-02: 查单恢复的订单同样可能 avgPrice=0, 走回填链
                    return await self._backfill_avg_price(_query_result, symbol)
            self._last_order_error = int(err_code) if str(err_code).lstrip('-').isdigit() else None
            logger.error(f"[Binance执行器] 市价单失败: {err_code} {err_msg}")
            return None

    async def place_limit_ioc_order(self, symbol: str, side: str, quantity: Decimal,
                                     price: Decimal, reduce_only: bool = False,
                                     position_side: Optional[str] = None) -> Optional[dict]:
        """下限价 IOC 单 (立即成交或取消)

        Args:
            symbol: 合约 symbol
            side: "BUY" 或 "SELL"
            quantity: 下单数量
            price: 限价
            reduce_only: 是否仅减仓

        Returns:
            订单回报 dict 或 None
        """
        qty = self._round_qty(symbol, quantity)
        px = self._round_price(symbol, price)

        # 检查最小下单量
        info = self.contract_info.get(symbol, {})
        min_qty = info.get("min_qty", Decimal('0.001'))
        if qty < min_qty:
            logger.warning(f"[Binance执行器] 下单数量 {qty} 小于最小值 {min_qty}，取消下单")
            return None

        # 🌟 P1-1: 生成 client_order_id 防重复下单
        client_oid = self._generate_client_order_id("ioc")
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "IOC",
            "quantity": str(qty),
            "price": str(px),
            "newClientOrderId": client_oid,
            "newOrderRespType": "RESULT",  # 返回撮合后的成交结果，避免 ACK 导致 executedQty 失真
        }
        _ps = (position_side or "").upper()
        if _ps in ("LONG", "SHORT", "BOTH"):
            params["positionSide"] = _ps
        if reduce_only and _ps not in ("LONG", "SHORT"):
            params["reduceOnly"] = "true"

        logger.info(f"[Binance执行器] 限价IOC单: {symbol} {side} 数量={qty} "
                    f"价格={px} positionSide={_ps or 'AUTO'} cid={client_oid}")

        result = await self.ws_client._rest_request("POST", "/fapi/v1/order", params, signed=True)
        if result and 'orderId' in result:
            # 🌟 2026-06-11 修复: 均价缺失回填 (同 place_market_order)
            result = await self._backfill_avg_price(result, symbol)
            order_id = result.get("orderId", "")
            status = result.get("status", "")
            filled = result.get("executedQty", "0")
            logger.info(f"[Binance执行器] IOC单结果: orderId={order_id} 状态={status} 已成交={filled}")
            return result
        # 🌟 P0-2 + E/A4: -5022 Duplicate 幂等恢复 + 仓位对账
        if isinstance(result, dict):
            err_code = result.get('code', '')
            if err_code and err_code not in (-5022, "-5022"):
                self._last_order_error = int(err_code) if str(err_code).lstrip('-').isdigit() else None
            if err_code in (-5022, "-5022"):
                logger.warning(f"[Binance执行器] IOC -5022 Duplicate, 查询真实订单...")
                _query_result = await self._query_order_by_client_oid(symbol, client_oid)
                if _query_result:
                    if _query_result.get('_uncertain_filled'):
                        logger.error(
                            f"[Binance执行器] 🚨 IOC -5022 查单全部失败, 触发仓位对账 "
                            f"(symbol={symbol} client_oid={client_oid})")
                        _recon = await self._reconcile_position_after_uncertain(
                            symbol, client_oid, side, qty)
                        return _recon
                    # 🌟 P2 修复: 防 client_oid 碰撞 — 校验 symbol/side/qty 是否与本请求一致
                    if not self._validate_recovered_order(_query_result, symbol, side, qty):
                        logger.error(
                            f"[Binance执行器] 🚨 IOC -5022 查单结果校验失败 (疑似 client_oid 碰撞), "
                            f"判本次下单失败; 交给上层重试")
                        return None
                    # 🌟 A4-1: 与 market 路径对齐, 查单恢复结果同样过均价回填链
                    return await self._backfill_avg_price(_query_result, symbol)
        return result

    async def cancel_order(self, symbol: str, order_id: int) -> Optional[dict]:
        """撤销订单

        Args:
            symbol: 合约 symbol
            order_id: 订单 ID

        Returns:
            撤单回报 dict 或 None
        """
        params = {
            "symbol": symbol,
            "orderId": str(order_id),
        }
        logger.info(f"[Binance执行器] 撤单: {symbol} orderId={order_id}")
        return await self.ws_client._rest_request("DELETE", "/fapi/v1/order", params, signed=True)

    async def close_position(self, symbol: str, position_side: Optional[str] = None) -> Optional[dict]:
        """平掉指定 symbol 的全部持仓

        自动检测持仓方向，下反向市价单平仓

        Returns:
            平仓订单回报 dict 或 None
        """
        # 优先从 WS 推送的持仓中获取
        _ps = (position_side or "").upper()
        pos = None
        if _ps in ("LONG", "SHORT"):
            pos = self.ws_client.positions_by_side.get((symbol, _ps))
        if pos is None:
            pos = self.ws_client.positions.get(symbol)
        if not pos or pos.quantity == 0:
            # 回退到 REST 查询
            risks = await self.ws_client.get_position_risk_all(symbol)
            if not risks:
                logger.warning(f"[Binance执行器] 无法获取 {symbol} 持仓信息")
                return None
            risk = None
            if _ps in ("LONG", "SHORT"):
                for r in risks:
                    if str(r.get("positionSide", "BOTH")).upper() == _ps:
                        risk = r
                        break
            if risk is None:
                risk = risks[0]
            pos_amt = Decimal(str(risk.get("positionAmt", "0")))
            if pos_amt == 0:
                logger.info(f"[Binance执行器] {symbol} 无持仓，无需平仓")
                return None
            if _ps not in ("LONG", "SHORT"):
                _risk_ps = str(risk.get("positionSide", "BOTH")).upper()
                if _risk_ps in ("LONG", "SHORT"):
                    _ps = _risk_ps
            if _ps == "LONG":
                side = "SELL"
            elif _ps == "SHORT":
                side = "BUY"
            else:
                side = "SELL" if pos_amt > 0 else "BUY"
            qty = abs(pos_amt)
        else:
            side = "SELL" if pos.side == "LONG" else "BUY"
            qty = pos.quantity
            if _ps not in ("LONG", "SHORT"):
                _ps = getattr(pos, "position_side", "BOTH")

        logger.info(f"[Binance执行器] 平仓: {symbol} 方向={side} 数量={qty} positionSide={_ps or 'AUTO'}")
        return await self.place_market_order(symbol, side, qty, reduce_only=True, position_side=_ps)

    async def hedge_order(self, symbol: str, side: str, quantity: Decimal,
                          order_type: str = "MARKET", max_slippage_usd: float = 5.0,
                          position_side: Optional[str] = None) -> Optional[dict]:
        """对冲下单主入口

        Args:
            symbol: Binance 合约 symbol
            side: "BUY" 或 "SELL"
            quantity: 对冲数量 (BTC)
            order_type: "MARKET" 或 "LIMIT_IOC"
            max_slippage_usd: 最大允许滑点 (USD)

        Returns:
            订单回报 dict 或 None
        """
        logger.info(f"[Binance执行器] 对冲下单: {symbol} {side} 数量={quantity} "
                     f"类型={order_type} 最大滑点=${max_slippage_usd}")
        self._last_order_error = None

        def _to_dec(v: Any, default: str = "0") -> Decimal:
            try:
                return Decimal(str(v if v is not None else default))
            except Exception:
                return Decimal(default)

        def _merge_with_fallback(primary: Optional[dict], fallback: Optional[dict],
                                 target_qty: Decimal, scene: str) -> Optional[dict]:
            """归一化 IOC+补单回报，避免上层把 EXPIRED(已部分成交)误判为失败。"""
            if not primary:
                return fallback

            merged = dict(primary)
            if fallback:
                merged["_fallback_order"] = fallback

            p_filled = _to_dec(primary.get("executedQty", "0"))
            f_filled = _to_dec(fallback.get("executedQty", "0")) if fallback else Decimal("0")
            total_filled = p_filled + f_filled

            p_quote = _to_dec(primary.get("cumQuote", primary.get("cummulativeQuoteQty", "0")))
            f_quote = _to_dec(fallback.get("cumQuote", fallback.get("cummulativeQuoteQty", "0"))) if fallback else Decimal("0")
            total_quote = p_quote + f_quote

            _target = target_qty if target_qty > 0 else _to_dec(primary.get("origQty", quantity))
            if _target <= 0:
                _target = quantity
            _step = Decimal(str(self.contract_info.get(symbol, {}).get("step_size", "0.001")))
            _tol = max(_step, Decimal("0.000001"))

            if total_filled > 0:
                merged["executedQty"] = str(total_filled)
                merged["origQty"] = str(_target)
                merged["status"] = "FILLED" if total_filled + _tol >= _target else "PARTIALLY_FILLED"
                # 🌟 API-R2-01 修复: 单腿 cumQuote 缺失时不得用总成交量稀释均价 —
                # 每腿有效报价额 = cumQuote 优先, 缺失用该腿 avgPrice×成交量推算;
                # 仍无价的腿不计入定价分母 (与结算 TWAP 的 priced-denominator 同思路)
                p_avg = _to_dec(primary.get("avgPrice", "0"))
                f_avg = _to_dec(fallback.get("avgPrice", "0")) if fallback else Decimal("0")
                p_q_eff = p_quote if p_quote > 0 else (p_avg * p_filled if p_avg > 0 else Decimal("0"))
                f_q_eff = f_quote if f_quote > 0 else (f_avg * f_filled if f_avg > 0 else Decimal("0"))
                _priced_filled = ((p_filled if p_q_eff > 0 else Decimal("0"))
                                  + (f_filled if f_q_eff > 0 else Decimal("0")))
                if _priced_filled > 0:
                    merged["avgPrice"] = str((p_q_eff + f_q_eff) / _priced_filled)
                # 🌟 A4-4: cumQuote 与 avgPrice 同口径 (含 avgPrice 推算的有效报价额),
                # 防下游 cumQuote/executedQty 推算得稀释价
                if (p_q_eff + f_q_eff) > 0:
                    merged["cumQuote"] = str(p_q_eff + f_q_eff)
                    merged["cummulativeQuoteQty"] = str(p_q_eff + f_q_eff)
                # 估算标记 (API-3/API-R2-05): 任一参与定价的腿为 mark_fallback 即保留标记,
                # 两腿均为真实价才清除
                _p_est = primary.get("avgPriceSource") == "mark_fallback" and p_q_eff > 0
                _f_est = bool(fallback) and (fallback or {}).get("avgPriceSource") == "mark_fallback" and f_q_eff > 0
                if _p_est or _f_est:
                    merged["avgPriceSource"] = "mark_fallback"
                else:
                    merged.pop("avgPriceSource", None)
                if fallback and f_filled > 0 and fallback.get("orderId"):
                    merged["orderId"] = fallback.get("orderId")
                logger.info(
                    f"[Binance执行器] {scene} 回报归一化: "
                    f"主单{p_filled} + 补单{f_filled} = 总成交{total_filled}/{_target} "
                    f"状态={merged.get('status')}")
            return merged

        # 🌟 P1 修复: 盘口新鲜度阈值 (秒) — WS 卡顿时陈旧价格会让护栏失效
        # Binance depth @100ms 推送, 2s 无更新大概率是网络/WS 异常, 此时应放弃护栏直接市价
        _OB_STALE_SEC = 2.0

        def _is_ob_fresh(_ob) -> bool:
            """检查 orderbook 是否新鲜 (update_time 在阈值内)"""
            if not _ob:
                return False
            _ut = getattr(_ob, 'update_time', 0.0) or 0.0
            if _ut <= 0:
                return False
            _age = time.time() - _ut
            if _age > _OB_STALE_SEC:
                logger.warning(
                    f"[Binance执行器] {symbol} 盘口陈旧 (age={_age:.2f}s > {_OB_STALE_SEC}s), "
                    f"放弃基于盘口的定价, 降级为市价单")
                return False
            return True

        if order_type == "MARKET":
            # MARKET 模式也必须受 max_slippage_usd 约束：
            # 优先下 LIMIT_IOC（价格护栏），仅在盘口不可用/IOC异常时降级为市价。
            # 🌟 P1 修复: 盘口陈旧时不用作护栏, 避免用过期价格导致保护价失真
            ob = self.ws_client.order_books.get(symbol)
            slippage = Decimal(str(max_slippage_usd))
            _ob_ok = _is_ob_fresh(ob)
            if _ob_ok and ob.best_bid is not None and ob.best_ask is not None and slippage > 0:
                if side.upper() == "BUY":
                    guard_price = ob.best_ask + slippage
                else:
                    guard_price = ob.best_bid - slippage

                logger.info(f"[Binance执行器] MARKET滑点护栏: 先尝试 LIMIT_IOC @ {guard_price}")
                guarded = await self.place_limit_ioc_order(
                    symbol, side, quantity, guard_price, position_side=position_side)
                if guarded:
                    filled_qty = Decimal(str(guarded.get("executedQty", "0")))
                    orig_qty = Decimal(str(guarded.get("origQty", quantity)))
                    if filled_qty == 0:
                        logger.warning("[Binance执行器] 滑点护栏 IOC 未成交，回退市价单补全")
                        return await self.place_market_order(symbol, side, quantity, position_side=position_side)
                    if filled_qty < orig_qty:
                        remaining = orig_qty - filled_qty
                        logger.warning(f"[Binance执行器] 滑点护栏 IOC 部分成交 {filled_qty}/{orig_qty}，"
                                       f"剩余 {remaining} 回退市价单补全")
                        fallback = await self.place_market_order(symbol, side, remaining, position_side=position_side)
                        return _merge_with_fallback(guarded, fallback, orig_qty, "MARKET护栏")
                    return _merge_with_fallback(guarded, None, orig_qty, "MARKET护栏")
                logger.warning("[Binance执行器] 滑点护栏 IOC 失败，回退市价单")
            else:
                logger.warning("[Binance执行器] 缺少盘口数据，MARKET模式无法启用滑点护栏，直接下市价")
            return await self.place_market_order(symbol, side, quantity, position_side=position_side)

        elif order_type == "LIMIT_IOC":
            # 获取当前最优价格
            # 🌟 P1 修复: 盘口新鲜度检查 — 陈旧盘口会让限价偏离实际市场, 扩大滑点
            ob = self.ws_client.order_books.get(symbol)
            if not ob or ob.best_bid is None or ob.best_ask is None:
                logger.warning(f"[Binance执行器] {symbol} 无盘口数据，回退至市价单")
                return await self.place_market_order(symbol, side, quantity, position_side=position_side)
            if not _is_ob_fresh(ob):
                # _is_ob_fresh 内部已记录陈旧告警, 直接回退市价 (MARKET 侧也会再次校验)
                return await self.place_market_order(symbol, side, quantity, position_side=position_side)

            slippage = Decimal(str(max_slippage_usd))
            if side.upper() == "BUY":
                # 买入：在 best_ask + slippage 处挂限价 IOC
                limit_price = ob.best_ask + slippage
            else:
                # 卖出：在 best_bid - slippage 处挂限价 IOC
                limit_price = ob.best_bid - slippage

            result = await self.place_limit_ioc_order(
                symbol, side, quantity, limit_price, position_side=position_side)

            # 检查是否完全成交
            if result:
                filled_qty = Decimal(str(result.get("executedQty", "0")))
                orig_qty = Decimal(str(result.get("origQty", "0")))

                if filled_qty < orig_qty and filled_qty > 0:
                    # 部分成交，剩余用市价单补全
                    remaining = orig_qty - filled_qty
                    logger.warning(f"[Binance执行器] IOC 部分成交 {filled_qty}/{orig_qty}，"
                                    f"剩余 {remaining} 用市价单补全")
                    fallback = await self.place_market_order(symbol, side, remaining, position_side=position_side)
                    result = _merge_with_fallback(result, fallback, orig_qty, "LIMIT_IOC")
                elif filled_qty > 0:
                    result = _merge_with_fallback(result, None, orig_qty, "LIMIT_IOC")

                elif filled_qty == 0:
                    # 完全未成交，回退市价单
                    logger.warning(f"[Binance执行器] IOC 完全未成交，回退至市价单")
                    result = await self.place_market_order(symbol, side, quantity, position_side=position_side)

            return result

        else:
            logger.error(f"[Binance执行器] 不支持的订单类型: {order_type}")
            return None
