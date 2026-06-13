"""deribit_client.py — Deribit WebSocket 客户端 (从 binance-deribit.py 提取)"""
import websockets
import asyncio
import time
import logging
import traceback
import itertools
import threading
import orjson
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict

import config
from utils import FastJSON, TokenBucketRateLimiter
from engine.models import MarketData, Position, Order, LocalOrderBook

from telegram_handler import tg_notifier

json = FastJSON()
logger = logging.getLogger(__name__)


def _maintenance_probe_interval_seconds() -> float:
    try:
        return max(float(config.BASE_CONFIG.get("maintenance_probe_interval_seconds", 300)), 60.0)
    except Exception:
        return 300.0


def _maintenance_probe_text() -> str:
    minutes = max(int(_maintenance_probe_interval_seconds() // 60), 1)
    return f"每 {minutes} 分钟"


class EnhancedDeribitWebSocketClient:
    """增强的WebSocket客户端，支持持仓管理和实时交易"""

    def __init__(self, client_id: str = None, client_secret: str = None, is_testnet: bool = True):
        # 测试网址
        self.ws_url = "wss://test.deribit.com/ws/api/v2" if is_testnet else "wss://www.deribit.com/ws/api/v2"
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.refresh_token = None
        self._token_refresh_task = None

        self.local_orderbooks: Dict[str, LocalOrderBook] = {}

        self.process_task = None

        # 连接状态
        self.ws = None
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5

        # 数据存储
        self.tickers: Dict[str, MarketData] = {}
        self.positions: Dict[str, Position] = {}
        self._last_positions_refresh_ok = False
        self._last_positions_refresh_ts = 0.0
        self.active_orders: Dict[str, Order] = {}
        self.order_history: List[Order] = []
        self._ws_unknown_order_warn_ts: Dict[str, float] = {}
        self.instrument_cache: Dict[str, dict] = {}

        # 请求ID计数器 (itertools.count 是原子性的，避免并发 ID 冲突)
        self._request_id_counter = itertools.count(1)

        # 订阅频道
        self.subscribed_channels = set()

        # 消息队列和响应映射
        self.message_queue = asyncio.Queue()
        self.pending_responses: Dict[int, asyncio.Future] = {}
        self.pending_lock = threading.Lock()
        self.listen_task = None
        # ================= 🌟 机构级升级：全局状态唤醒铃铛 =================
        self.state_condition = asyncio.Condition()
        # =================================================================
        # ================= 🌟 修复：初始化 30次/秒 的限速器 =================
        self.rate_limiter = TokenBucketRateLimiter(calls_per_second=30)
        # =================================================================
        # ================= 🌟 WebSocket消息序列号检测 =================
        # 用于检测消息丢包和乱序
        self.ws_sequence_numbers: Dict[str, int] = {}  # {channel: last_sequence}
        self.ws_message_gaps = 0  # 丢包统计
        # =================================================================

        # 从配置读取币种
        self.target_currency = config.target_currency if hasattr(config, 'target_currency') else "BTC"

        # 维护熔断状态。必须初始化为显式字段，避免动态属性导致恢复路径漏判。
        self.maintenance_sleep_active = False
        self._maintenance_notified = False
        self._maintenance_cooldown_task = None
        # 🌟 2026-06-11: 下单失败日志节流 + 最近错误记录 (供开仓冷却分类) + 疑似维护滑窗
        self._order_error_log_ts: Dict[str, float] = {}
        self.recent_order_errors: Dict[str, Tuple[float, str]] = {}
        self._suspect_maintenance_events: List[Tuple[float, str]] = []

    def _store_order_snapshot(self, order: Order) -> None:
        """Store an order in the right local cache according to its current state."""
        if not order or not order.order_id:
            return
        terminal_status = {'filled', 'cancelled', 'rejected'}
        status = str(order.status or '').lower()
        if status in terminal_status:
            self.active_orders.pop(order.order_id, None)
            for idx in range(len(self.order_history) - 1, -1, -1):
                if self.order_history[idx].order_id == order.order_id:
                    self.order_history[idx] = order
                    break
            else:
                self.order_history.append(order)
            if len(self.order_history) > 5000:
                self.order_history = self.order_history[-5000:]
        else:
            self.active_orders[order.order_id] = order

    def _order_from_api_data(self, order_data: dict, existing: Optional[Order] = None,
                             fallback_amount: Optional[Decimal] = None,
                             fallback_price: Optional[Decimal] = None) -> Order:
        """Build an Order snapshot from Deribit order payload, preserving existing metadata when absent."""
        def _dec_or(value, default):
            if value in (None, ''):
                return default
            try:
                return Decimal(str(value))
            except Exception:
                return default

        order_id = order_data.get('order_id') or (existing.order_id if existing else '')
        price_raw = order_data.get('price')
        amount_raw = order_data.get('amount')
        amount_default = fallback_amount if fallback_amount is not None else (
            existing.amount if existing else Decimal('0'))
        price_default = fallback_price if fallback_price is not None else (
            existing.price if existing else None)
        filled_default = existing.filled_amount if existing else Decimal('0')
        avg_default = existing.average_price if existing else Decimal('0')
        _ro_raw = order_data.get('reduce_only')
        reduce_only_val = bool(_ro_raw) if _ro_raw is not None else (
            existing.reduce_only if existing else False)
        return Order(
            order_id=order_id,
            instrument_name=order_data.get('instrument_name') or (existing.instrument_name if existing else ''),
            side=order_data.get('direction') or (existing.side if existing else ''),
            amount=_dec_or(amount_raw, amount_default),
            price=_dec_or(price_raw, price_default),
            order_type=order_data.get('order_type') or (existing.order_type if existing else 'limit'),
            label=order_data.get('label') or (existing.label if existing else ''),
            status=order_data.get('order_state') or (existing.status if existing else 'open'),
            timestamp=existing.timestamp if existing else time.time(),
            filled_amount=_dec_or(order_data.get('filled_amount'), filled_default),
            average_price=_dec_or(order_data.get('average_price'), avg_default),
            reduce_only=reduce_only_val
        )


    async def start_listening(self):
        """启动消息监听器"""
        self.listen_task = asyncio.create_task(self._message_listener())

    async def stop_listening(self):
        """停止消息监听器"""
        if self.listen_task:
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass

    async def _message_listener(self):
        """单一的消息监听器"""
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)

                    # 处理心跳 — 零延迟异步发射回复，不阻塞 listener 循环
                    if data.get('method') == 'heartbeat':
                        if data.get('params', {}).get('type') == 'test_request':
                            asyncio.create_task(self._reply_heartbeat())
                        continue

                    # 处理有ID的响应（RPC响应）
                    if 'id' in data:
                        request_id = data['id']
                        future = None

                        # 获取并移除future
                        with self.pending_lock:
                            future = self.pending_responses.pop(request_id, None)

                        if future is not None and not future.done():
                            future.set_result(data)
                        else:
                            # 放入消息队列供其他处理器处理
                            await self.message_queue.put(data)
                    else:
                        # 订阅消息放入队列
                        await self.message_queue.put(data)

                except orjson.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"处理消息异常: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket连接关闭")
            self.is_connected = False
            # 立即暂停交易，防止用过时数据做决策
            if tg_notifier.engine:
                tg_notifier.engine._add_pause("Deribit WS断连")
            asyncio.create_task(tg_notifier.notify_network_disconnect())
        except Exception as e:
            logger.error(f"消息监听器异常: {e}")
            self.is_connected = False
            # 立即暂停交易，防止用过时数据做决策
            if tg_notifier.engine:
                tg_notifier.engine._add_pause("Deribit WS断连")
            asyncio.create_task(tg_notifier.send_error_async(str(e)[:200], "ws_listener_error"))

    async def send_request(self, msg: dict, is_private: bool = False, timeout: float = 10.0) -> dict:
        """发送请求并获取响应"""
        # ================= 🌟 修复：前置断连检查，避免 WS 已死时堆积大量 10s 超时请求 =================
        if not self.is_connected:
            return {'error': {'message': 'WS disconnected'}}
        # =========================================================================================
        try:
            # 生成请求ID
            request_id = self._get_next_request_id()
            msg['id'] = request_id

            if is_private and self.access_token:
                if 'params' not in msg:
                    msg['params'] = {}
                msg['params']['access_token'] = self.access_token

            # 创建Future来等待响应
            with self.pending_lock:
                future = asyncio.Future()
                self.pending_responses[request_id] = future
            # ================= 🌟 修复：发单前获取令牌，平滑瞬间并发 =================
            await self.rate_limiter.acquire()
            # =======================================================================
            # 发送请求
            await self.ws.send(json.dumps(msg))

            # 等待响应或超时
            try:
                response = await asyncio.wait_for(future, timeout)
                return response
            except asyncio.TimeoutError:
                # 清理pending response
                with self.pending_lock:
                    if request_id in self.pending_responses:
                        future_to_cancel = self.pending_responses.pop(request_id)
                        if not future_to_cancel.done():
                            future_to_cancel.cancel()
                if msg.get('method') == 'private/get_order_state':
                    logger.debug(f"主动查询状态延迟，已跳过 (可忽略)")
                else:
                    logger.error(f"请求超时: {msg.get('method')}")
                return {'error': {'message': 'Timeout'}}
            except asyncio.CancelledError:
                # 处理取消
                with self.pending_lock:
                    if request_id in self.pending_responses:
                        future_to_cancel = self.pending_responses.pop(request_id)
                        if not future_to_cancel.done():
                            future_to_cancel.cancel()
                raise

        except Exception as e:
            logger.error(f"发送请求异常: {e}")
            # 网络断开类错误不重复发 Telegram（会由 ConnectionClosed 统一通知）
            err_str = str(e).lower()
            if 'close frame' not in err_str and 'connection' not in err_str and 'closed' not in err_str:
                asyncio.create_task(tg_notifier.send_error_async(str(e)[:200], "api_request_error"))
            return {'error': {'message': str(e)}}

    async def process_messages(self):
        """处理消息队列中的消息"""
        while self.is_connected:
            try:
                # 非阻塞获取消息
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                # 处理消息
                if 'params' in message and 'channel' in message['params']:
                    channel = message['params']['channel']
                    if any(keyword in channel for keyword in ['user.changes', 'user.orders']):
                        await self.process_user_data(message['params'])
                    else:
                        await self._process_market_data(message['params'])

            except Exception as e:
                logger.warning(f"处理消息队列异常: {e}")

    async def _reply_heartbeat(self):
        """立即回复 Deribit test_request 心跳 — 最高优先级，不经过 rate_limiter"""
        try:
            response = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "public/test",
                "params": {}
            }
            await self.ws.send(json.dumps(response))
        except Exception as e:
            logger.warning(f"心跳回复发送失败: {e}")

    def _get_next_request_id(self) -> int:
        """获取下一个请求ID (itertools.count 保证原子性)"""
        return next(self._request_id_counter)

    async def connect_with_retry(self):
        """带重试的连接"""
        # 每次被外部引擎调用要求重新连接时，必须将重试计数器强制清零！否则会陷入瞬间判定失败的 5 秒死循环
        self.reconnect_attempts = 0
        while self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                await self._connect()
                # 连接成功 → 如果之前是维护状态，标记维护结束
                if getattr(self, 'maintenance_sleep_active', False):
                    logger.info("✅ Deribit WS 连接成功，维护可能已结束")
                return True
            except Exception as e:
                self.reconnect_attempts += 1
                err_str = str(e).lower()

                # 🌟 维护检测: HTTP 502/503 是 Deribit 维护的典型特征
                # 旧逻辑: 只在 send_request (locked_by_admin) 时检测维护
                # 新逻辑: WS 连接被拒时也检测，避免 5 次重试刷屏
                if ('502' in err_str or '503' in err_str or
                        'service unavailable' in err_str or
                        'bad gateway' in err_str):
                    if tg_notifier.engine:
                        is_first_maintenance = not getattr(self, 'maintenance_sleep_active', False)
                        if is_first_maintenance:
                            self.maintenance_sleep_active = True
                            tg_notifier.engine._add_pause("Deribit维护")
                        self._start_maintenance_cooldown(source="http", is_first_maintenance=is_first_maintenance)
                    return False  # 不继续重试，交给维护休眠机制

                wait_time = min(2 ** self.reconnect_attempts, 30)
                logger.warning(
                    f"连接失败 (尝试 {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}, {wait_time}秒后重试")
                await asyncio.sleep(wait_time)

        logger.error("达到最大重试次数，连接失败")
        # 仅首次失败发送 Telegram 通知，后续重试失败静默（由主循环的断线重连机制持续重试）
        if not getattr(self, '_ws_fail_notified', False):
            self._ws_fail_notified = True
            asyncio.create_task(tg_notifier.send_async(
                "❌ 【网络异常】Deribit WebSocket 连接失败！\n"
                "系统已自动暂停交易，将持续重试。网络恢复后将自动通知。"))
        return False

    def _start_maintenance_cooldown(self, source: str = "locked_by_admin", is_first_maintenance: bool = False) -> None:
        """Start one maintenance probe loop if none is running."""
        task = getattr(self, '_maintenance_cooldown_task', None)
        if task and not task.done():
            return
        self._maintenance_cooldown_task = asyncio.create_task(
            self._maintenance_cooldown_loop(source=source, is_first_maintenance=is_first_maintenance)
        )

    async def _maintenance_probe_once(self) -> bool:
        """Probe Deribit availability using a short-lived WS, independent of the trading WS."""
        try:
            async with websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=60,
                close_timeout=10,
                open_timeout=30,
                max_size=None
            ) as probe_ws:
                if self.client_id and self.client_secret:
                    auth_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_request_id(),
                        "method": "public/auth",
                        "params": {
                            "grant_type": "client_credentials",
                            "client_id": self.client_id,
                            "client_secret": self.client_secret
                        }
                    }
                    await probe_ws.send(json.dumps(auth_msg))
                    auth_resp = json.loads(await asyncio.wait_for(probe_ws.recv(), timeout=10.0))
                    access_token = auth_resp.get('result', {}).get('access_token')
                    if not access_token:
                        return False
                    pos_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_request_id(),
                        "method": "private/get_positions",
                        "params": {"currency": self.target_currency, "access_token": access_token}
                    }
                    await probe_ws.send(json.dumps(pos_msg))
                    pos_resp = json.loads(await asyncio.wait_for(probe_ws.recv(), timeout=10.0))
                    if 'result' not in pos_resp or 'error' in pos_resp:
                        return False

                    # Read-only endpoints can recover before trading is unlocked.
                    # Use cancel-all as a safe write-class probe: it cannot open risk,
                    # and it should still be allowed once Deribit trading is usable.
                    cancel_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_request_id(),
                        "method": "private/cancel_all_by_currency",
                        "params": {"currency": self.target_currency, "access_token": access_token}
                    }
                    await probe_ws.send(json.dumps(cancel_msg))
                    cancel_resp = json.loads(await asyncio.wait_for(probe_ws.recv(), timeout=10.0))
                    return 'result' in cancel_resp and 'error' not in cancel_resp

                time_msg = {
                    "jsonrpc": "2.0",
                    "id": self._get_next_request_id(),
                    "method": "public/get_time",
                    "params": {}
                }
                await probe_ws.send(json.dumps(time_msg))
                time_resp = json.loads(await asyncio.wait_for(probe_ws.recv(), timeout=10.0))
                return 'result' in time_resp and 'error' not in time_resp
        except Exception:
            return False

    async def probe_maintenance_recovery_once(self, notify: bool = True) -> bool:
        """Return True and clear maintenance pause if an independent Deribit probe succeeds."""
        if not await self._maintenance_probe_once():
            return False
        self.maintenance_sleep_active = False
        self._maintenance_notified = False
        if tg_notifier.engine:
            tg_notifier.engine._remove_pause("Deribit维护")
        if notify:
            asyncio.create_task(tg_notifier.send_async(
                "✅ 【维护结束】Deribit 已恢复，系统正在自动重连并恢复交易"))
        return True

    async def _maintenance_cooldown_loop(self, source: str = "locked_by_admin", is_first_maintenance: bool = False):
        if is_first_maintenance:
            if source == "http":
                logger.warning("🛑 检测到 Deribit 系统维护 (HTTP 502/503)！进入维护休眠模式。")
                asyncio.create_task(tg_notifier.send_async(
                    "🛑 【维护检测】Deribit 返回 HTTP 502/503\n"
                    f"系统进入维护休眠，将{_maintenance_probe_text()}自动试探。\n"
                    "维护结束后自动恢复并通知。"))
            else:
                logger.warning(f"🛑 触发系统维护保护！进入 {_maintenance_probe_text()}安全休眠...")
                asyncio.create_task(tg_notifier.send_error_async(
                    "🛑 【维护熔断触发】检测到 Deribit 系统维护 (locked_by_admin)！\n"
                    f"系统进入安全休眠，{_maintenance_probe_text()}自动试探。维护结束后将自动恢复并通知。",
                    "exchange_maintenance"
                ))

        while self.maintenance_sleep_active:
            await asyncio.sleep(_maintenance_probe_interval_seconds())
            if await self.probe_maintenance_recovery_once(notify=True):
                return


    async def _connect(self, silent: bool = False):
        """建立WebSocket连接"""
        if not silent:
            logger.info(f"正在连接到 {self.ws_url}")
        self.ws = await websockets.connect(
            self.ws_url,
            ping_interval=30,  # 心跳间隔
            ping_timeout=60,  # 容忍心跳超时长达 60 秒
            close_timeout=10,
            open_timeout=30,  # 容忍握手建立阶段的卡顿高达 30 秒 (默认仅10秒)
            max_size=None  # 允许接收极大深度数据包防爆内存
        )

        if not silent:
            logger.info("WebSocket连接已建立")

        # 认证（必须在设置 is_connected 之前完成，否则主循环误判连接正常）
        if self.client_id and self.client_secret:
            auth_ok = await self.authenticate(silent=silent)
            if not auth_ok:
                raise ConnectionError("WebSocket认证失败，将重试")

        self.is_connected = True
        return True

    async def cleanup(self):
        """清理资源"""
        # 清空陈旧行情数据，防止重连后新推送到达前使用过期价格做决策
        self.tickers.clear()
        self.local_orderbooks.clear()

        # 取消 Token 刷新任务
        if self._token_refresh_task and not self._token_refresh_task.done():
            self._token_refresh_task.cancel()
            try:
                await self._token_refresh_task
            except asyncio.CancelledError:
                pass
        # 取消所有pending的futures
        with self.pending_lock:
            for request_id, future in list(self.pending_responses.items()):
                if not future.done():
                    future.cancel()
            self.pending_responses.clear()

        # 停止监听任务
        if self.listen_task:
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass
        # ==== 杀死队列处理僵尸任务 ====
        if hasattr(self, 'process_task') and self.process_task:
            self.process_task.cancel()

    async def authenticate(self, silent: bool = False):
        """WebSocket认证（走统一 send_request 通道，避免与 _message_listener 竞争 recv）"""
        try:
            if not silent:
                logger.info("WebSocket正在认证...")
            msg = {
                "jsonrpc": "2.0",
                "method": "public/auth",
                "params": {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
            }

            # ================= 修复：统一走 send_request 通道 =================
            # 首次认证时 _message_listener 可能尚未启动，需要判断：
            # 如果 listener 已启动，走 send_request；否则走原始 ws.send + ws.recv
            if self.listen_task and not self.listen_task.done():
                # Listener 已运行，必须走统一通道
                res = await self.send_request(msg, is_private=False, timeout=10.0)
            else:
                # Listener 未启动（首次连接），安全直接 recv
                await self.ws.send(json.dumps(msg))
                response = await asyncio.wait_for(self.ws.recv(), timeout=10)
                res = json.loads(response)
            # =================================================================

            if 'result' in res and 'access_token' in res['result']:
                self.access_token = res['result']['access_token']
                self.refresh_token = res['result'].get('refresh_token')
                expires_in = res['result'].get('expires_in', 900)
                if not silent:
                    logger.info(f"WebSocket认证成功, Token 有效期: {expires_in}s")
                # 启动自动续期任务
                self._schedule_token_refresh(expires_in)

                # ================= 🌟 修复：启用 Deribit 应用层心跳 =================
                # 仅靠 websockets 库的 TCP ping/pong 不够，Deribit 服务端需要应用层心跳
                # 来维持连接活性。未启用时连接会被交易所静默丢弃 (no close frame)。
                try:
                    hb_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_request_id(),
                        "method": "public/set_heartbeat",
                        "params": {"interval": 30}
                    }
                    if self.listen_task and not self.listen_task.done():
                        await self.send_request(hb_msg, is_private=False, timeout=5.0)
                    else:
                        await self.ws.send(json.dumps(hb_msg))
                        _ = await asyncio.wait_for(self.ws.recv(), timeout=5)  # 清空响应缓冲区
                    if not silent:
                        logger.info("✅ 已启用 Deribit 应用层心跳 (interval=30s)")
                except Exception as hb_e:
                    if not silent:
                        logger.warning(f"⚠️ 启用应用层心跳失败（不影响连接）: {hb_e}")
                # =================================================================

                return True
            else:
                error_msg = res.get('error', {}).get('message', '未知错误')
                if not silent:
                    logger.error(f"认证失败: {error_msg}")
                    asyncio.create_task(tg_notifier.send_error_async(f"认证失败: {error_msg}", "auth_failed"))
                self.access_token = None
                self.refresh_token = None
                return False

        except asyncio.TimeoutError:
            if not silent:
                logger.error("认证超时")
                asyncio.create_task(tg_notifier.send_error_async("认证失败: 认证超时", "auth_failed"))
            self.access_token = None
            self.refresh_token = None
            return False
        except Exception as e:
            if not silent:
                logger.error(f"认证异常: {e}")
                asyncio.create_task(tg_notifier.send_error_async(f"认证失败: {e}", "auth_failed"))
            self.access_token = None
            self.refresh_token = None
            return False

    def _schedule_token_refresh(self, expires_in: int):
        """启动 Token 自动续期定时器 (在到期前 60 秒刷新)"""
        if self._token_refresh_task and not self._token_refresh_task.done():
            self._token_refresh_task.cancel()
        refresh_delay = max(expires_in - 60, 30)
        self._token_refresh_task = asyncio.create_task(self._token_refresh_loop(refresh_delay))

    async def _token_refresh_loop(self, delay: float):
        """Token 续期循环

        🌟 Bug#7 修复：不再仅依赖 is_connected 控制循环退出。
        authenticate() 调用期间 is_connected 可能短暂为 False，
        改为 task 被 cancel 或连续失败 10 次才彻底退出。
        """
        _consecutive_failures = 0
        _max_total_failures = 10  # 累计失败上限，防止无限重试
        try:
            while True:
                await asyncio.sleep(delay)
                if not self.is_connected:
                    # 连接断开时等待重连，而非直接退出
                    logger.warning("Token 续期: 检测到连接断开，等待 30s 后重试...")
                    delay = 30
                    _consecutive_failures += 1
                    if _consecutive_failures >= _max_total_failures:
                        logger.error(f"Token 续期: 累计失败 {_max_total_failures} 次，退出续期循环")
                        asyncio.create_task(tg_notifier.send_error_async("Token 续期累计失败次数过多，续期循环已退出", "token_loop_exit"))
                        break
                    continue
                logger.info("Token 即将过期，正在自动续期...")
                try:
                    if self.refresh_token:
                        msg = {
                            "jsonrpc": "2.0",
                            "method": "public/auth",
                            "params": {
                                "grant_type": "refresh_token",
                                "refresh_token": self.refresh_token
                            }
                        }
                    else:
                        msg = {
                            "jsonrpc": "2.0",
                            "method": "public/auth",
                            "params": {
                                "grant_type": "client_credentials",
                                "client_id": self.client_id,
                                "client_secret": self.client_secret
                            }
                        }
                    # 使用 send_request 走统一通道，避免与 _message_listener 抢 recv()
                    res = await self.send_request(msg, is_private=False, timeout=10.0)
                    if 'result' in res and 'access_token' in res['result']:
                        self.access_token = res['result']['access_token']
                        self.refresh_token = res['result'].get('refresh_token', self.refresh_token)
                        new_expires = res['result'].get('expires_in', 900)
                        delay = max(new_expires - 60, 30)
                        _consecutive_failures = 0
                        logger.info(f"Token 续期成功, 下次续期在 {delay}s 后")
                    else:
                        _consecutive_failures += 1
                        logger.warning(f"Token 续期响应异常 (连续失败 {_consecutive_failures} 次)，30s 后重试")
                        delay = 30
                except Exception as e:
                    _consecutive_failures += 1
                    logger.error(f"Token 续期失败 (连续失败 {_consecutive_failures} 次): {e}，30s 后重试")
                    delay = 30

                # 连续失败 3 次：清空旧 token，降级为从头认证
                if _consecutive_failures >= 3 and self.is_connected:
                    logger.error("Token 续期连续失败 3 次，清空旧 token 并尝试重新认证...")
                    asyncio.create_task(tg_notifier.send_error_async("Token 续期连续失败，已触发重认证", "token_refresh_failed"))
                    self.access_token = None
                    self.refresh_token = None
                    auth_ok = await self.authenticate()
                    if auth_ok:
                        _consecutive_failures = 0
                        delay = max(delay, 60)
                        logger.info("重新认证成功，恢复续期循环")
                    else:
                        logger.error("重新认证也失败，将在 30s 后继续重试")
                        asyncio.create_task(tg_notifier.send_error_async("Token 续期重认证也失败，连接可能不可用", "token_reauth_failed"))
                        delay = 30

                if _consecutive_failures >= _max_total_failures:
                    logger.error(f"Token 续期: 累计失败 {_max_total_failures} 次，退出续期循环")
                    asyncio.create_task(tg_notifier.send_error_async("Token 续期累计失败次数过多，续期循环已退出", "token_loop_exit"))
                    break
        except asyncio.CancelledError:
            pass

    async def get_positions(self, currency: str = "BTC", kind: str = None, silent: bool = False) -> List[Position]:
        """获取持仓信息 (加入 silent 静默参数防刷屏)"""
        try:
            if not silent:
                logger.info(f"正在获取{currency}持仓信息...")
            params = {"currency": currency}
            if kind:
                params["kind"] = kind

            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/get_positions",
                "params": params
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                self._last_positions_refresh_ok = False
                error_msg = response['error'].get('message', '未知错误')
                if 'disconnected' in error_msg.lower():
                    logger.info(f"获取持仓跳过: WS 已断开，等待重连")
                    return []
                logger.error(f"获取持仓失败: {error_msg}")
                # asyncio.create_task(tg_notifier.send_error_async(f"获取持仓失败: {error_msg}", "position_fetch_failed"))
                return []

            positions = []
            # 🌟 F1 修复: REST 全量刷新前先记录旧 key 集合
            # 刷新后删除 REST 未返回的条目（已结算/已平仓的幽灵数据）
            # 防止重连后 client.positions 残留过期的 delta/pnl 导致风控误判
            _old_keys = set(self.positions.keys())
            _new_keys = set()
            _parse_failed = False
            for item in response.get('result', []):
                try:
                    position = Position(
                        instrument_name=item['instrument_name'],
                        size=Decimal(str(item.get('size', 0))),
                        average_price=Decimal(str(item.get('average_price', 0))),
                        mark_price=Decimal(str(item.get('mark_price', 0))),
                        realized_pnl=Decimal(str(item.get('total_profit_loss', 0))),
                        # 🌟 RISK-5 修复: 使用 floating_profit_loss（仅未实现盈亏），避免已平仓实现盈亏污染全局止损判断
                        unrealized_pnl=Decimal(str(item.get('floating_profit_loss', item.get('total_profit_loss', 0)))),
                        timestamp=time.time(),
                        delta=Decimal(str(item.get('delta', 0))),
                        gamma=Decimal(str(item.get('gamma', 0)))
                    )
                    _new_keys.add(position.instrument_name)
                    if position.size != Decimal('0'):
                        positions.append(position)
                    self.positions[position.instrument_name] = position
                except Exception as e:
                    _parse_failed = True
                    logger.warning(f"解析持仓失败: {item}, 错误: {e}")

            # 🌟 F1 修复: 清除 REST 未返回的旧条目（已结算/消失的幽灵持仓）
            _stale = _old_keys - _new_keys
            for _sk in _stale:
                _old_pos = self.positions.get(_sk)
                # 仅清除 size=0 或 REST 确认不存在的条目
                # 保留 WS 推送中更新过的（时间戳更新表示仍活跃）
                if _old_pos and (time.time() - getattr(_old_pos, 'timestamp', 0)) > 30:
                    del self.positions[_sk]
                    if not silent:
                        logger.info(f"🧹 清除过期持仓缓存: {_sk} (REST 未返回)")

            if not silent:
                logger.info(f"成功获取 {len(positions)} 个持仓")
            for p in positions:
                direction = "🟢 多" if p.size > 0 else "🔴 空"
                if not silent:
                    logger.info(f"【持仓】{p.instrument_name} | {direction} | 数量: {abs(p.size)} | 均价: {p.average_price:.2f} | 浮动盈亏: {p.unrealized_pnl:.5f}")

            self._last_positions_refresh_ok = not _parse_failed
            self._last_positions_refresh_ts = time.time()
            return positions

        except Exception as e:
            self._last_positions_refresh_ok = False
            logger.error(f"获取持仓异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"获取持仓异常: {e}", "position_fetch_failed"))
            return []

    async def _force_refresh_positions_async(self):
        """WebSocket消息丢包后强制刷新持仓（异步包装，防止阻塞）"""
        try:
            await asyncio.sleep(0.1)  # 短暂延迟，避免与其他操作冲突
            await self.get_positions(self.target_currency, silent=True)
            logger.info("✅ 因WS丢包，已强制刷新持仓状态")
        except Exception as e:
            logger.warning(f"强制刷新持仓异常: {e}")

    async def get_open_orders(self, currency: str = "BTC") -> List[Order]:
        """获取活跃订单"""
        try:
            logger.info("正在获取活跃订单...")
            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/get_open_orders_by_currency",
                "params": {"currency": currency}
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                return []

            orders = []
            for item in response.get('result', []):
                try:
                    # 🌟 2026-06-11 事故修复: 必须传 existing — close_position 创建的订单
                    # 在交易所侧无 label, 不传 existing 会用空 label 覆盖本地 'em_close' 标记,
                    # 导致 _find_active_deribit_close_order 失配 → invalid_reduce_only_order 死循环
                    order = self._order_from_api_data(
                        item, existing=self.active_orders.get(item.get('order_id') or ''))
                    orders.append(order)
                    self._store_order_snapshot(order)
                except Exception as e:
                    logger.warning(f"解析订单失败: {item}, 错误: {e}")

            return orders

        except Exception as e:
            logger.error(f"获取活跃订单异常: {e}")
            return []

    def get_order_by_id(self, order_id: str) -> Optional[Order]:
        """根据订单ID安全地获取订单最新状态 (用于 Excel 数据统计)"""
        # 先在活跃订单中找
        if order_id in self.active_orders:
            return self.active_orders[order_id]
        # 再到历史订单中找
        for o in reversed(self.order_history):
            if o.order_id == order_id:
                return o
        return None

    async def get_instrument_info(self, instrument_name: str) -> Optional[Dict]:
        """获取合约信息（带本地缓存，防止触发 API 频率限制）"""
        # 1. 如果缓存里有，直接从内存秒回，不发网络请求
        if instrument_name in self.instrument_cache:
            return self.instrument_cache[instrument_name]

        # 2. 如果缓存没有，再发起网络请求
        try:
            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "public/get_instrument",
                "params": {"instrument_name": instrument_name}
            }

            response = await self.send_request(msg)

            if 'error' in response or 'result' not in response:
                logger.error(f"获取合约信息失败: {instrument_name}")
                return None

            # 3. 将结果存入缓存
            self.instrument_cache[instrument_name] = response['result']
            return response['result']
        except Exception as e:
            logger.error(f"获取合约信息异常: {e}")
            return None

    @staticmethod
    def _adjust_to_tick_size(price: Decimal, tick_size: Decimal) -> Decimal:
        """将价格对齐到最小变动价位"""
        if tick_size <= Decimal('0'):
            return price

        # 计算最接近的tick价格
        tick_count = price / tick_size
        rounded_ticks = round(tick_count)
        adjusted_price = rounded_ticks * tick_size

        # 确保至少保留8位小数
        return adjusted_price.quantize(Decimal('0.00000001'))

    @staticmethod
    def _get_dynamic_tick(price: Decimal, _inst_info: dict = None) -> Decimal:
        """Deribit BTC/ETH 期权分档 tick。

        优先解析 API 返回的 tick_size_steps (实测 get_instrument(s) 均含该字段,
        当前为 [{'tick_size': 0.0005, 'above_price': 0.005}] + tick_size=0.0001),
        交易所未来调档时自动适配; 解析失败回落硬编码两档 (与现行官方规则一致):
        价格 ≤0.005 → 0.0001，价格 >0.005 → 0.0005。"""
        try:
            if _inst_info:
                _steps = _inst_info.get('tick_size_steps') or []
                _base_tick = Decimal(str(_inst_info.get('tick_size', '0.0001')))
                if _steps:
                    # 按 above_price 降序找首个 price > above_price 的档位
                    _parsed = sorted(
                        ((Decimal(str(s.get('above_price'))), Decimal(str(s.get('tick_size'))))
                         for s in _steps if s.get('above_price') is not None and s.get('tick_size')),
                        key=lambda x: x[0], reverse=True)
                    for _above, _tick in _parsed:
                        if price > _above:
                            return _tick
                    return _base_tick
        except Exception:
            pass
        if price <= Decimal('0.005'):
            return Decimal('0.0001')
        else:
            return Decimal('0.0005')

    def _is_future_instrument(self, instrument_name: str) -> bool:
        """判断是否为期货合约"""
        # BTC期货命名格式：BTC-13FEB26 或 BTC-PERPETUAL
        return instrument_name.startswith(f"{self.target_currency}-") and (
            instrument_name.endswith("PERPETUAL") or
            (len(instrument_name.split('-')) == 2 and instrument_name.split('-')[1].isalnum())
        )

    @staticmethod
    def _is_option_instrument(instrument_name: str) -> bool:
        """判断是否为期权合约"""
        # BTC期权命名格式：BTC-13FEB26-77000-C 或 BTC-13FEB26-77000-P
        parts = instrument_name.split('-')
        return len(parts) == 4 and parts[3] in ['C', 'P', 'CALL', 'PUT']

    # ================= 机构级风控：硬编码安全上限 (不可被 Telegram 热更新覆盖) =================
    MAX_OPTION_ORDER_SIZE_BTC = Decimal('10.0')      # 单笔期权最大报单量 10 BTC
    MAX_FUTURE_ORDER_SIZE_USD = Decimal('1000000')   # 单笔期货最大报单面值 100万 USD
    # ==================================================================================

    async def place_order(self, instrument_name: str, amount: Decimal, side: str, order_type: str = "limit",
                          price: Optional[Decimal] = None, label: str = "", is_maker: bool = False,
                          log_prefix: str = "", reduce_only=False, time_in_force: str = "good_til_cancelled") ->Optional[Order]:
        """下单（带并发上下文日志前缀）"""
        prefix = f"{log_prefix}" if log_prefix else ""
        try:
            logger.info(f"{prefix}下单: {instrument_name} {side} {amount} @ {price if price else 'market'} ({order_type})")

            # ================= 机构级事前风控: 最大报单量限制 (Max Order Size Guard) =================
            is_future = self._is_future_instrument(instrument_name)
            if is_future:
                if amount > self.MAX_FUTURE_ORDER_SIZE_USD:
                    logger.error(
                        f"{prefix}🚨 [报单量拦截] 期货面值 {amount} USD 超过硬上限 {self.MAX_FUTURE_ORDER_SIZE_USD} USD，订单已拒绝！")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 报单量拦截: {instrument_name} 面值 {amount} USD 超硬上限", "max_size_block"))
                    return None
            else:
                if amount > self.MAX_OPTION_ORDER_SIZE_BTC:
                    logger.error(
                        f"{prefix}🚨 [报单量拦截] 期权数量 {amount} 超过硬上限 {self.MAX_OPTION_ORDER_SIZE_BTC}，订单已拒绝！")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 报单量拦截: {instrument_name} 数量 {amount} 超硬上限", "max_size_block"))
                    return None
            # ==================================================================================

            # ==== 添加市场深度对比日志 ====
            ticker = self.tickers.get(instrument_name)
            if ticker and order_type == "limit" and price:
                if side == "buy":
                    logger.info(
                        f"{prefix}买限价: {price} | 当前卖一价: {ticker.ask} | 价差: {ticker.ask - price}")
                    if price >= ticker.ask:
                        logger.info(f"{prefix}[注意] 买限价单价格 >= 卖一价，将作为Taker立即成交！")
                else:  # sell
                    logger.info(
                        f"{prefix}卖限价: {price} | 当前买一价: {ticker.bid} | 价差: {price - ticker.bid}")
                    if price <= ticker.bid:
                        logger.info(f"{prefix}[注意] 卖限价单价格 <= 买一价，将作为Taker立即成交！")

            instrument_info = await self.get_instrument_info(instrument_name)
            if not instrument_info:
                logger.error(f"{prefix}无法获取合约信息: {instrument_name}")
                return None

            # 期权动态阶梯 Tick Size：解析 API 返回的 tick_size_steps（BTC/ETH 分档 tick）
            if self._is_option_instrument(instrument_name):
                check_price = price if price else Decimal('0')
                tick_size = self._get_dynamic_tick(check_price, instrument_info)
            else:
                tick_size = Decimal(str(instrument_info.get('tick_size', '0.5')))

            # 处理价格精度
            if price and price > Decimal('0'):
                original_price = price
                price = self._adjust_to_tick_size(price, tick_size)
                if original_price != price:
                    logger.info(f"{prefix}价格对齐(跳点 {tick_size}): {original_price} -> {price}")

            actual_amount = amount
            is_future = self._is_future_instrument(instrument_name)

            if is_future:
                min_trade_amount = Decimal(str(instrument_info.get('min_trade_amount', 10)))
                if actual_amount < min_trade_amount:
                    logger.warning(f"{prefix}期货下单面值调整: {actual_amount} -> {min_trade_amount}")
                    actual_amount = min_trade_amount
                if min_trade_amount > Decimal('0'):
                    actual_amount = (actual_amount / min_trade_amount).quantize(Decimal('0'),
                                                                                rounding='ROUND_UP') * min_trade_amount
                logger.info(f"{prefix}期货面值(USD): {actual_amount}")

            params = {
                "instrument_name": instrument_name,
                "amount": float(actual_amount),
                "type": order_type,
                "side": side,
                "label": label
            }

            if order_type == "limit":
                if price:
                    params["price"] = float(price)
                # 🌟 修复：接收上层传来的 IOC 等高级指令
                params["time_in_force"] = time_in_force
                params["post_only"] = is_maker

            # if is_maker:
            #     logger.info(f"{prefix}[Maker模式] Post-Only已启用")

            if reduce_only:
                params["reduce_only"] = True

            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/" + ("sell" if side == "sell" else "buy"),
                "params": params
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                error_msg = str(response['error'].get('message', '未知错误'))
                error_code = response['error'].get('code', '')
                error_data = response['error'].get('data', {})
                # 🌟 核心修复：提取隐藏的真实原因 (2026-06-11: 补记数字错误码, 便于区分子原因)
                real_reason = error_data.get('reason', '')
                full_error = f"{error_msg} | {real_reason} | code={error_code}"

                # 🌟 2026-06-11: 记录最近错误供开仓冷却分类 (交易所拒单 vs 良性撤单)
                _err_key_short = (real_reason or error_msg or 'unknown')[:60]
                self.recent_order_errors[instrument_name] = (time.time(), _err_key_short)
                if len(self.recent_order_errors) > 200:
                    _cutoff = time.time() - 600
                    self.recent_order_errors = {
                        k: v for k, v in self.recent_order_errors.items() if v[0] >= _cutoff}

                # 🌟 2026-06-11: 同 (合约,方向,错误) 60s 节流, 防止重试循环 ERROR 刷屏
                # (2.5小时 359 条 invalid_reduce_only_order 事故)。首条必发;
                # 注意: 下方保证金/维护副作用分支不受节流影响, 必须无条件执行。
                _log_key = f"{instrument_name}:{side}:{_err_key_short}"
                _now_err = time.time()
                _last_err_log = self._order_error_log_ts.get(_log_key, 0.0)
                _should_log_error = (_now_err - _last_err_log) >= 60.0
                if _should_log_error:
                    self._order_error_log_ts[_log_key] = _now_err
                    if len(self._order_error_log_ts) > 300:
                        _cutoff2 = _now_err - 3600
                        self._order_error_log_ts = {
                            k: v for k, v in self._order_error_log_ts.items() if v >= _cutoff2}
                _maint_probe_reject = (
                    ('locked_by_admin' in full_error.lower() or 'maintenance' in full_error.lower())
                    and getattr(self, 'maintenance_sleep_active', False))
                if 'settlement_in_progress' in full_error.lower():
                    logger.warning(f"{prefix}下单失败: {full_error}")
                elif _maint_probe_reject:
                    # 维护休眠期间的真实订单探测被拒是预期结果 (说明维护未结束), 降级 warning
                    logger.warning(f"{prefix}下单失败(维护探测): {full_error}")
                elif _should_log_error:
                    logger.error(f"{prefix}下单失败: {full_error}")
                else:
                    logger.debug(f"{prefix}下单失败(节流重复): {full_error}")

                # ================= 🌟 保证金不足：暂停开仓，保留平仓监控 =================
                if 'not_enough_funds' in full_error.lower() or 'insufficient' in full_error.lower():
                    if tg_notifier.engine and not getattr(tg_notifier.engine, '_margin_shutdown_active', False):
                        asyncio.create_task(tg_notifier.engine._margin_emergency_shutdown(
                            "Deribit", f"下单被拒: {full_error}"))

                # ================= 🌟 终极修复：真实订单 5 分钟休眠探测法 =================
                _is_maintenance_error = (
                    'locked_by_admin' in full_error.lower() or 'maintenance' in full_error.lower())

                # 🌟 2026-06-11: invalid_args_for_instrument 疑似维护检测
                # 事故证据: 测试网维护准备/扫尾期对锁定合约返回 invalid_args_for_instrument
                # (同一订单在维护深度期返回 locked_by_admin, 参数实测完全合规)。
                # 判据: 120s 内 ≥3 次且涉及 ≥2 个不同合约 → 按维护处理 (5分钟休眠探测)。
                # ⚠️ 误判代价 (RISK-4, 与真实维护的既有行为一致): 维护暂停会使
                # monitor_positions 整体跳过巡检 (含硬止损/Gamma), 且恢复探针的
                # cancel_all 会撤掉在途平仓挂单 (由破损组合重试循环+60s 重发兜底);
                # probe 成功后 ~5 分钟自动解除, 比 45s 无告警重试风暴更安全。
                if not _is_maintenance_error and 'invalid_args_for_instrument' in full_error.lower():
                    _now_sm = time.time()
                    self._suspect_maintenance_events.append((_now_sm, instrument_name))
                    self._suspect_maintenance_events = [
                        (t, inst) for t, inst in self._suspect_maintenance_events
                        if _now_sm - t <= 120.0]
                    _distinct_insts = {inst for _, inst in self._suspect_maintenance_events}
                    if len(self._suspect_maintenance_events) >= 3 and len(_distinct_insts) >= 2:
                        logger.error(
                            f"{prefix}🚨 [疑似维护] invalid_args_for_instrument 120s内 "
                            f"{len(self._suspect_maintenance_events)} 次/涉及 {len(_distinct_insts)} 个合约, "
                            f"按交易所维护降级处理")
                        _is_maintenance_error = True
                        self._suspect_maintenance_events = []

                if _is_maintenance_error:
                    if tg_notifier.engine:
                        if not getattr(self, 'maintenance_sleep_active', False):
                            self.maintenance_sleep_active = True
                            tg_notifier.engine._add_pause("Deribit维护")

                        # 仅首次进入维护时发送 Telegram 通知；但每次都确保 cooldown 任务存在。
                        is_first_maintenance = not getattr(self, '_maintenance_notified', False)
                        self._maintenance_notified = True

                        self._start_maintenance_cooldown(source="locked_by_admin", is_first_maintenance=is_first_maintenance)

                return None

            result = response.get('result', {})
            if result:
                # 下单成功 → 如果之前在维护状态，说明维护已结束，发送恢复通知
                if getattr(self, '_maintenance_notified', False):
                    self._maintenance_notified = False
                    logger.info("✅ Deribit 系统维护已结束，交易恢复正常")
                    asyncio.create_task(tg_notifier.send_async(
                        "✅ 【维护结束】Deribit 系统维护已结束，交易已自动恢复正常运行！"))

                order_data = result.get('order', {})
                order = self._order_from_api_data(
                    order_data,
                    fallback_amount=actual_amount,
                    fallback_price=price
                )
                # Ensure local metadata is populated even if Deribit omits it in the response.
                order.instrument_name = order.instrument_name or instrument_name
                order.side = order.side or side
                order.order_type = order.order_type or order_type
                order.label = order.label or label
                self._store_order_snapshot(order)

                logger.info(f"{prefix}下单成功: {instrument_name} {side} {actual_amount} @ {order.average_price if order.average_price > 0 else (price if price else 'market')} | ID: {order.order_id} | 状态: {order.status} | 已成交: {order.filled_amount}")

                if order_type == "limit" and price and ticker:
                    if side == "buy":
                        position = "在买一价" if price == ticker.bid else "高于买一价" if price > ticker.bid else "低于买一价"
                    else:
                        position = "在卖一价" if price == ticker.ask else "低于卖一价" if price < ticker.ask else "高于卖一价"
                    logger.info(f"{prefix}ID: {order.order_id} {position} @{price:.4f}")

                return order
            return None
        except Exception as e:
            logger.error(f"{prefix}下单异常: {e}", exc_info=True)
            return None

    async def edit_order(self, order_id: str, amount: Optional[Decimal] = None, price: Optional[Decimal] = None, log_prefix: str = "") -> bool:
        """修改订单（带前缀）"""
        prefix = f"{log_prefix}" if log_prefix else ""
        try:
            params: Dict[str, Any] = {"order_id": order_id}
            if amount: params["amount"] = float(amount)
            if price: params["price"] = float(price)

            msg = {"jsonrpc": "2.0", "id": self._get_next_request_id(), "method": "private/edit", "params": params}
            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                error_msg = str(response['error'].get('message', '未知错误'))
                error_data = response['error'].get('data', {})
                # 🌟 核心修复：提取隐藏的真实原因
                real_reason = error_data.get('reason', '')
                full_error = f"{error_msg} | {real_reason}"

                logger.info(f"{prefix}改单失败: {full_error}")
                # ================= 🌟 终极修复：真实订单 5 分钟休眠探测法 =================
                if 'locked_by_admin' in full_error.lower() or 'maintenance' in full_error.lower():
                    if tg_notifier.engine:
                        if not getattr(self, 'maintenance_sleep_active', False):
                            self.maintenance_sleep_active = True
                            tg_notifier.engine._add_pause("Deribit维护")

                        is_first_maintenance = not getattr(self, '_maintenance_notified', False)
                        self._maintenance_notified = True

                        self._start_maintenance_cooldown(source="locked_by_admin", is_first_maintenance=is_first_maintenance)
                # =========================================================================
                return False

            result = response.get('result', {})
            order_data = result.get('order', {}) if isinstance(result, dict) else {}
            existing = self.get_order_by_id(order_id)
            if order_data:
                order = self._order_from_api_data(
                    order_data,
                    existing=existing,
                    fallback_amount=amount,
                    fallback_price=price
                )
                self._store_order_snapshot(order)
            elif existing:
                if amount is not None:
                    existing.amount = amount
                if price is not None:
                    existing.price = price
                self._store_order_snapshot(existing)
            logger.info(f"{prefix}改单成功: {order_id} -> 新价格: {price}")
            return True
        except Exception as e:
            logger.error(f"{prefix}改单异常: {e}")
            return False

    async def cancel_order(self, order_id: str, log_prefix: str = "") -> bool:
        """取消订单（带前缀）"""
        prefix = f"{log_prefix}" if log_prefix else ""
        try:
            logger.info(f"{prefix}撤单请求: {order_id}")
            msg = {"jsonrpc": "2.0", "id": self._get_next_request_id(), "method": "private/cancel",
                   "params": {"order_id": order_id}}
            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                error_msg = str(response['error'].get('message', '未知错误'))
                error_data = response['error'].get('data', {})
                # 🌟 核心修复：提取隐藏的真实原因
                real_reason = error_data.get('reason', '')
                full_error = f"{error_msg} | {real_reason}"

                # not_open_order 说明订单已成交或已被撤销，属于预期内的正常情况
                if 'not_open_order' in full_error:
                    logger.info(f"{prefix}撤单跳过(订单已不在挂单簿): {order_id}")
                    _order = self.active_orders.pop(order_id, None)
                    _synced = False
                    try:
                        _state_msg = {
                            "jsonrpc": "2.0", "id": self._get_next_request_id(),
                            "method": "private/get_order_state",
                            "params": {"order_id": order_id}
                        }
                        _state_resp = await self.send_request(_state_msg, is_private=True, timeout=2.0)
                        if 'result' in _state_resp:
                            _real_state = _state_resp['result'].get('order_state') or 'cancelled'
                            if str(_real_state).lower() not in {'filled', 'cancelled', 'rejected'}:
                                _real_state = 'cancelled'
                            _real_filled = Decimal(str(_state_resp['result'].get('filled_amount', 0)))
                            _real_avg = Decimal(str(_state_resp['result'].get('average_price', 0) or 0))
                            if _order:
                                _order.status = _real_state
                                _order.filled_amount = _real_filled
                                if _real_avg > 0:
                                    _order.average_price = _real_avg
                                self._store_order_snapshot(_order)
                                _synced = True
                                logger.info(
                                    f"{prefix}not_open_order 状态同步: {_real_state}, "
                                    f"成交量={_real_filled}, 均价={_real_avg}")
                            else:
                                for _hist in reversed(self.order_history):
                                    if _hist.order_id == order_id:
                                        _hist.status = _real_state
                                        _hist.filled_amount = _real_filled
                                        if _real_avg > 0:
                                            _hist.average_price = _real_avg
                                        _synced = True
                                        logger.info(
                                            f"{prefix}not_open_order 历史订单状态更新: {_real_state}, "
                                            f"成交量={_real_filled}, 均价={_real_avg}")
                                        break
                                if not _synced:
                                    logger.debug(
                                        f"{prefix}not_open_order 本地历史未命中: {order_id} | "
                                        f"真实状态={_real_state}, 成交量={_real_filled}, 均价={_real_avg}")
                    except Exception as _e:
                        logger.debug(f"{prefix}not_open_order 状态查询失败(可忽略): {_e}")
                    if _order and not _synced:
                        # not_open_order 已证明该订单不再是活单；即使 REST 查状态失败，
                        # 也必须从 active_orders 移走，避免 L2/ghost 路径把 stale 活单当真。
                        _order.status = 'cancelled'
                        self._store_order_snapshot(_order)
                else:
                    logger.error(f"{prefix}撤单失败: {full_error}")
                return False

            # 🌟 关键修复：从 cancel 响应中提取真实 filled_amount，防止部分成交被吞
            # Deribit cancel 成功时返回完整订单对象，包含截止撤单时刻的真实成交量
            result_data = response.get('result', {})
            resp_filled = Decimal(str(result_data.get('filled_amount', 0)))

            if order_id in self.active_orders:
                order = self.active_orders.pop(order_id)
                order.status = 'cancelled'
                # 用 cancel 响应的真实值覆盖可能过期的 WS 值
                if resp_filled > order.filled_amount:
                    logger.warning(f"{prefix}⚠️ 撤单发现隐藏成交！WS记录: {order.filled_amount}, 实际: {resp_filled}")
                    order.filled_amount = resp_filled
                    order.average_price = Decimal(str(result_data.get('average_price', order.average_price)))
                self.order_history.append(order)

            logger.info(f"{prefix}撤单成功: {order_id}")

            # ========== 🌟 机构级修复：撤单后幽灵成交确认机制 ==========
            # 防止撤单请求与成交撮合的竞态条件导致的幽灵成交（ghost fill）
            # 场景：撤单请求发出后，交易所实际成交但WS消息丢包，导致裸腿风险
            await asyncio.sleep(0.5)  # 等待撮合引擎稳定
            try:
                verify_msg = {
                    "jsonrpc": "2.0",
                    "id": self._get_next_request_id(),
                    "method": "private/get_order_state",
                    "params": {"order_id": order_id}
                }
                verify_resp = await self.send_request(verify_msg, is_private=True, timeout=2.0)

                if 'result' in verify_resp:
                    final_state = verify_resp['result'].get('order_state')
                    final_filled = Decimal(str(verify_resp['result'].get('filled_amount', 0)))

                    # 检测幽灵成交：撤单后实际变为filled状态
                    if final_state == 'filled' and final_filled > 0:
                        logger.error(f"{prefix}🚨 幽灵成交检测！订单 {order_id} 撤单后实际成交 {final_filled}")
                        # 更新order_history中的记录
                        for hist_order in reversed(self.order_history):
                            if hist_order.order_id == order_id:
                                hist_order.status = 'filled'
                                hist_order.filled_amount = final_filled
                                hist_order.average_price = Decimal(str(verify_resp['result'].get('average_price', hist_order.average_price)))
                                logger.warning(f"{prefix}⚡ 幽灵成交已记录到历史，等待上层处理逻辑识别")
                                break
                        # 返回False以便上层知道这不是简单的撤单
                        return False
                    elif final_filled > resp_filled:
                        # 部分成交增量
                        logger.warning(f"{prefix}⚠️ 撤单后成交量增加: {resp_filled} -> {final_filled}")
                        for hist_order in reversed(self.order_history):
                            if hist_order.order_id == order_id:
                                hist_order.filled_amount = final_filled
                                hist_order.average_price = Decimal(str(verify_resp['result'].get('average_price', hist_order.average_price)))
                                break
            except Exception as verify_err:
                logger.debug(f"{prefix}撤单后状态确认异常(可忽略): {verify_err}")
            # =================================================================

            return True
        except Exception as e:
            logger.error(f"{prefix}撤单异常: {e}")
            asyncio.create_task(tg_notifier.send_error_async(f"撤单异常: {e}", "cancel_order_error"))
            return False

    async def cancel_all_orders(self, currency: str = "BTC", silent: bool = False) -> bool:
        """取消所有订单"""
        try:
            if not silent:
                logger.info(f"取消所有{currency}订单")

            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/cancel_all_by_currency",
                "params": {"currency": currency}
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                error_msg = str(response['error'].get('message', '未知错误'))
                error_data = response['error'].get('data', {})
                # 🌟 核心修复：提取隐藏的真实原因
                real_reason = error_data.get('reason', '')
                full_error = f"{error_msg} | {real_reason}"

                if not silent:
                    logger.error(f"取消所有订单失败: {full_error}")
                    asyncio.create_task(tg_notifier.send_error_async(f"取消所有订单失败: {full_error}", "cancel_all_failed"))
                return False

            # 批量撤单后逐单确认最终状态，防止 cancel 与成交竞态导致隐藏成交量丢失。
            _orders_to_archive = list(self.active_orders.values())
            for _ao in _orders_to_archive:
                _final_order = _ao
                try:
                    _state_msg = {
                        "jsonrpc": "2.0",
                        "id": self._get_next_request_id(),
                        "method": "private/get_order_state",
                        "params": {"order_id": _ao.order_id}
                    }
                    _state_resp = await self.send_request(_state_msg, is_private=True, timeout=2.0)
                    if isinstance(_state_resp, dict) and 'result' in _state_resp:
                        _final_order = self._order_from_api_data(_state_resp['result'], existing=_ao)
                        _state = str(_final_order.status or '').lower()
                        if _state not in {'filled', 'cancelled', 'rejected'}:
                            _final_order.status = 'cancelled'
                    else:
                        _final_order.status = 'cancelled'
                except Exception as _state_err:
                    if not silent:
                        logger.debug(f"批量撤单后订单状态确认失败(可忽略): {_ao.order_id} {_state_err}")
                    _final_order.status = 'cancelled'
                self._store_order_snapshot(_final_order)
            if not silent:
                logger.info("取消所有订单成功")
            return True

        except Exception as e:
            if not silent:
                logger.error(f"取消所有订单异常: {e}")
                asyncio.create_task(tg_notifier.send_error_async(f"取消所有订单异常: {e}", "cancel_all_failed"))
            return False

    async def subscribe_positions(self, currency: str = "BTC"):
        """订阅持仓变化"""
        try:
            # 订阅期货持仓变化
            future_channel = f"user.changes.future.{currency}.100ms"
            # 订阅期权持仓变化
            option_channel = f"user.changes.option.{currency}.100ms"

            channels = [future_channel, option_channel]

            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/subscribe",
                "params": {"channels": channels}
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                logger.warning(f"订阅持仓变化失败: {response['error'].get('message')}")
            else:
                logger.info("订阅持仓变化成功")
                self.subscribed_channels.update(channels)

        except Exception as e:
            logger.error(f"订阅持仓变化异常: {e}")

    async def subscribe_orders(self, currency: str = "BTC"):
        """订阅订单变化"""
        try:
            channel = f"user.orders.{currency}.100ms"

            msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/subscribe",
                "params": {"channels": [channel]}
            }

            response = await self.send_request(msg, is_private=True)

            if 'error' in response:
                logger.warning(f"订阅订单变化失败: {response['error'].get('message')}")
            else:
                logger.info("订阅订单变化成功")
                self.subscribed_channels.add(channel)

        except Exception as e:
            logger.error(f"订阅订单变化异常: {e}")

    async def process_user_data(self, params: dict):
        """处理用户数据更新（持仓和订单）"""
        try:
            channel = params.get('channel', '')
            data = params.get('data', {})

            # ========== 🌟 WebSocket消息序列号检测 ==========
            # Deribit某些频道会包含序列号，用于检测消息丢包
            if 'prev_change_id' in data and 'change_id' in data:
                current_seq = data.get('change_id')
                prev_seq = data.get('prev_change_id')

                # 检查序列号连续性
                if channel in self.ws_sequence_numbers:
                    expected_prev = self.ws_sequence_numbers[channel]
                    if prev_seq != expected_prev:
                        gap = current_seq - expected_prev - 1
                        self.ws_message_gaps += gap
                        logger.warning(
                            f"⚠️ WS消息丢包检测！频道: {channel}, "
                            f"预期prev: {expected_prev}, 实际prev: {prev_seq}, "
                            f"当前seq: {current_seq}, 丢失: {gap}条"
                        )
                        # 丢包后强制刷新持仓（防止状态不一致）
                        asyncio.create_task(self._force_refresh_positions_async())

                self.ws_sequence_numbers[channel] = current_seq
            # ==================================================

            # 🌟 核心修复：Deribit 的 user.changes 频道会同时塞入 positions 和 orders
            if 'user.changes' in channel:
                # 必须同时处理两者！
                await self._process_position_change(data)
                await self._process_order_change(data)

            # 兼容单独订阅 user.orders 频道的情况
            elif 'user.orders' in channel:
                await self._process_order_change(data)

        except Exception as e:
            logger.warning(f"处理用户数据异常: {e}")

    async def _process_position_change(self, data: dict):
        """处理持仓变化"""
        try:
            for position_data in data.get('positions', []):
                instrument_name = position_data.get('instrument_name')
                size = Decimal(str(position_data.get('size', 0)))

                if size == 0:
                    # 持仓为0，从内存中移除
                    if instrument_name in self.positions:
                        del self.positions[instrument_name]
                else:
                    # 更新或添加持仓
                    position = Position(
                        instrument_name=instrument_name,
                        size=size,
                        average_price=Decimal(str(position_data.get('average_price', 0))),
                        mark_price=Decimal(str(position_data.get('mark_price', 0))),
                        realized_pnl=Decimal(str(position_data.get('total_profit_loss', 0))),
                        # 🌟 RISK-5 修复: 使用 floating_profit_loss（仅未实现盈亏）
                        unrealized_pnl=Decimal(str(position_data.get('floating_profit_loss', position_data.get('total_profit_loss', 0)))),
                        timestamp=time.time(),
                        # 👇 提取官方风控引擎的 Delta 和 Gamma
                        delta=Decimal(str(position_data.get('delta', 0))),
                        gamma=Decimal(str(position_data.get('gamma', 0)))
                    )
                    self.positions[instrument_name] = position

            logger.debug(f"持仓更新: {len(data.get('positions', []))}个持仓")

        except Exception as e:
            logger.warning(f"处理持仓变化异常: {e}")

    async def _process_order_change(self, data: dict):
        """处理订单变化"""
        try:
            for order_data in data.get('orders', []):
                order_id = order_data.get('order_id')
                order_state = order_data.get('order_state')

                if order_id in self.active_orders:
                    order = self.active_orders[order_id]
                    ws_filled = Decimal(str(order_data.get('filled_amount', 0)))

                    # ========== 🌟 关键订单状态REST API二次确认 ==========
                    # 对锚定腿（Maker）的filled状态进行二次确认，防止WS消息错误
                    # 识别锚定腿：label中包含"anchor"或订单类型为post_only
                    is_critical_order = (
                        'anchor' in order.label.lower() or
                        order_state == 'filled' and order.order_type == 'limit'
                    )

                    if is_critical_order and order_state == 'filled' and ws_filled > 0:
                        # 异步发起REST API确认，避免阻塞WS消息处理
                        asyncio.create_task(
                            self._verify_critical_order_fill(order_id, order_state, ws_filled, order.label)
                        )
                    # ==================================================

                    order.status = order_state
                    order.filled_amount = ws_filled
                    order.average_price = Decimal(str(order_data.get('average_price', 0)))

                    # 如果订单已完成或取消，移动到历史
                    if order_state in ['filled', 'cancelled', 'rejected']:
                        self.active_orders.pop(order_id)
                        self.order_history.append(order)
                        # ====== 【新增】防止内存溢出 ======
                        if len(self.order_history) > 5000:
                            self.order_history = self.order_history[-5000:]
                        logger.info(f"订单{order_state}: {order_id}")
                else:
                    # 🌟 关键修复：订单已被 cancel_order() 移入 history 后，WS 通知到达时
                    # 仍需更新 filled_amount，防止 cancel 与 fill 竞态导致成交量丢失
                    ws_filled = Decimal(str(order_data.get('filled_amount', 0)))
                    if ws_filled > 0:
                        found = False
                        for hist_order in reversed(self.order_history):
                            if hist_order.order_id == order_id:
                                if ws_filled > hist_order.filled_amount:
                                    logger.warning(f"⚠️ WS 延迟通知补录: {order_id} 成交量 {hist_order.filled_amount} -> {ws_filled}")
                                    hist_order.filled_amount = ws_filled
                                    hist_order.average_price = Decimal(str(order_data.get('average_price', hist_order.average_price)))
                                    hist_order.status = order_state
                                found = True
                                break
                        if not found and ws_filled > 0:
                            # 订单可能是 close_position 返回后未入缓存或 history 被截断。
                            # 仅对终态订单或本程序清理/套利标签订单补录到缓存，避免未知 open
                            # 订单进入 active_orders 后抑制幽灵仓位检测。
                            _state = str(order_state or '').lower()
                            _label = str(order_data.get('label') or '').lower()
                            _terminal = _state in {'filled', 'cancelled', 'rejected'}
                            _ours = _label.startswith((
                                'arb_', 'em_', 'fb_', 'stop_all_', 'l2a_', 'l2t_', 'l2f_', 'ghost_'))
                            if _terminal or _ours:
                                recovered_order = self._order_from_api_data(order_data)
                                self._store_order_snapshot(recovered_order)
                            _now = time.time()
                            _last = self._ws_unknown_order_warn_ts.get(order_id, 0.0)
                            if _now - _last >= 300:
                                self._ws_unknown_order_warn_ts[order_id] = _now
                                logger.warning(
                                    f"⚠️ WS 延迟通知补录未知订单: {order_id} "
                                    f"(状态={order_state}, 成交={ws_filled}, "
                                    f"{'已补录' if (_terminal or _ours) else '未缓存非本程序非终态订单'})")
            # ================= 🌟 机构级升级：摇响铃铛唤醒策略 =================
            async with self.state_condition:
                self.state_condition.notify_all()
            # =================================================================

        except Exception as e:
            logger.warning(f"处理订单变化异常: {e}")

    async def _verify_critical_order_fill(self, order_id: str, ws_state: str, ws_filled: Decimal, label: str):
        """
        关键订单成交REST API二次确认
        防止WebSocket消息错误导致的状态不一致
        """
        try:
            await asyncio.sleep(0.3)  # 短暂延迟，让撮合引擎稳定

            verify_msg = {
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "private/get_order_state",
                "params": {"order_id": order_id}
            }
            verify_resp = await self.send_request(verify_msg, is_private=True, timeout=2.0)

            if 'result' in verify_resp:
                real_state = verify_resp['result'].get('order_state')
                real_filled = Decimal(str(verify_resp['result'].get('filled_amount', 0)))

                # 检测状态不一致
                if real_state != ws_state or abs(real_filled - ws_filled) > Decimal('0.001'):
                    logger.error(
                        f"🚨 关键订单状态不一致！{order_id} [{label}]\n"
                        f"   WS: {ws_state} / {ws_filled} | REST: {real_state} / {real_filled}"
                    )
                    # 更新为REST API的真实状态
                    for hist_order in reversed(self.order_history):
                        if hist_order.order_id == order_id:
                            hist_order.status = real_state
                            hist_order.filled_amount = real_filled
                            hist_order.average_price = Decimal(str(verify_resp['result'].get('average_price', hist_order.average_price)))
                            logger.warning(f"✅ 已用REST真实状态覆盖WS错误状态: {order_id}")
                            break
                else:
                    logger.info(f"✅ 关键订单状态确认一致: {order_id}")
        except Exception as e:
            logger.debug(f"关键订单二次确认异常(可忽略): {e}")

    async def _process_market_data(self, params: dict):
        """处理市场数据"""
        channel = params['channel']
        data = params.get('data', {})

        # 处理ticker数据
        if channel.startswith('ticker.'):
            instrument = channel.split('.')[1]
            ticker_data = MarketData(
                bid=Decimal(str(data.get('best_bid_price', 0))),
                ask=Decimal(str(data.get('best_ask_price', 0))),
                bid_size=Decimal(str(data.get('best_bid_amount', 0))),
                ask_size=Decimal(str(data.get('best_ask_amount', 0))),
                min_price=Decimal(str(data.get('min_price', 0))),
                max_price=Decimal(str(data.get('max_price', 0))),
                timestamp=time.time()
            )
            self.tickers[instrument] = ticker_data
        # ====== 机构级升级：处理本地增量订单簿数据 (Raw LOB) + 丢包检测 ======
        elif channel.startswith('book.'):
            instrument = channel.split('.')[1]
            if instrument not in self.local_orderbooks:
                self.local_orderbooks[instrument] = LocalOrderBook()
            book = self.local_orderbooks[instrument]
            ok = book.update(data)
            if not ok:
                # 序列号不连续 → 丢包！重新订阅获取新 snapshot
                _prev = data.get('prev_change_id')
                _last = book.last_change_id
                logger.warning(
                    f"⚠️ [OrderBook丢包] {instrument} prev_change_id={_prev} != last={_last}，"
                    f"重新订阅获取 snapshot...")
                book.bids.clear()
                book.asks.clear()
                book.last_change_id = None
                # 异步重新订阅（unsubscribe + subscribe 触发新 snapshot）
                asyncio.create_task(self._resubscribe_book(instrument))
        # ================= 🌟 机构级升级：摇响铃铛唤醒策略 =================
        async with self.state_condition:
            self.state_condition.notify_all()
        # =================================================================

    async def _resubscribe_book(self, instrument: str):
        """OrderBook 丢包后重新订阅：先取消订阅再重新订阅，触发交易所发送新的 snapshot"""
        channel = f"book.{instrument}.raw"
        try:
            await self.send_request({
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "public/unsubscribe",
                "params": {"channels": [channel]}
            }, timeout=5.0)
            await asyncio.sleep(0.1)
            await self.send_request({
                "jsonrpc": "2.0",
                "id": self._get_next_request_id(),
                "method": "public/subscribe",
                "params": {"channels": [channel]}
            }, timeout=5.0)
            logger.info(f"✅ [OrderBook恢复] {instrument} 已重新订阅，等待新 snapshot")
        except Exception as e:
            logger.error(f"[OrderBook恢复] {instrument} 重订阅失败: {e}")

    async def close(self):
        self.is_connected = False
        if self.ws:
            await self.ws.close()
