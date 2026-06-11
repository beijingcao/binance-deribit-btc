"""engine/core.py — 引擎核心: __init__ + 属性 + 暂停管理 (从 binance-deribit.py 提取)"""
from __future__ import annotations
import logging
import asyncio
import time
from decimal import Decimal
from typing import Dict, Tuple, Set, Optional

import redis.asyncio as redis
import config
import db_store
from deribit_client import EnhancedDeribitWebSocketClient
from fee_calculator import FeeCalculator
from trade_executor import TradeExecutor
from engine.models import ArbitrageState
from telegram_handler import tg_notifier

logger = logging.getLogger(__name__)


class RealTimeArbitrageEngineCore:
    """实时套利引擎 - 核心属性与暂停管理"""

    def __init__(self, client_id: str = None, client_secret: str = None, fee_tier: str = 'standard', is_testnet: bool = True):
        self.client = EnhancedDeribitWebSocketClient(client_id, client_secret, is_testnet)
        # 传入 fee_tier 参数
        self.fee_calculator = FeeCalculator(tier=fee_tier)
        self.trade_executor = TradeExecutor(self.client, self.fee_calculator)
        self.trade_executor.engine = self
        # ================= 暂停状态管理 (集合驱动) =================
        # 每个子系统添加/移除自己的原因，互不干扰。集合非空即暂停。
        self._pause_reasons: set = set()  # 活跃暂停原因集合
        self._manual_stop = False       # 手动 stop 命令标记，只有 start 命令才能解除
        # 🛡️ 修复 C: 引擎启动时间戳，用于 _auto_close_naked_legs 启动冷启动窗保护
        # 0.0 = 未启动；run() 起始处赋值为 time.time()
        self._engine_start_ts: float = 0.0
        self._exit_attempt_notified = set()  # 平仓尝试已通知的组合，防止重复打印
        self._last_pnl_log_time = {}  # PnL 日志冷却时间戳
        self.target_currency = "BTC"
        self.min_profit_threshold = Decimal('20.0')
        self.max_option_dte_hours = 72  # 只交易距到期≤72小时(3天)的期权
        self.min_option_dte_hours = 12  # 距到期低于该值不开新仓（可配置，默认12小时）
        self.trade_amount = Decimal('0.1')
        self.futures_numbers = 3
        self.max_wait_time = 60

        self.moneyness_threshold = Decimal('0.15')
        self.max_spread_pct = Decimal('0.10')
        self.concurrent_batch_size = 5
        self.batch_interval = 0.5
        self.scan_interval_ms = 1000

        # 套利组合存储
        self.arbitrage_combinations: Dict[Tuple[str, Decimal], Dict[str, str]] = {}
        # 【新增】缓存合约面值，key是合约名称，value是面值
        self.contract_sizes: Dict[str, Decimal] = {}

        # 状态管理
        self.arbitrage_states: Dict[Tuple[str, Decimal], ArbitrageState] = {}
        self.processing_opportunities: Set[Tuple[str, Decimal]] = set()  # 冷却锁
        self.position_locks: Set[Tuple[str, Decimal]] = set()  # 持仓锁
        # 🌟 B 修复: per-combo 平仓互斥锁, 防止 _handle_delivery_settlement 与
        # _emergency_dump_all 被 monitor_positions 同一秒内并发触发, 造成重复关 Binance 对冲
        # key: state.expiry_strike (与 arbitrage_states 同粒度)
        self._combo_closing_locks: Dict[Tuple[str, Decimal], asyncio.Lock] = {}
        # 🌟 C 修复: 标记"Deribit 期权已结算但 Binance 对冲关单失败"的组合, 防止 monitor
        # 下一秒再次进入 _handle_delivery_settlement 绕过保护逻辑形成裸腿
        self._binance_close_failed_combos: Set[Tuple[str, Decimal]] = set()

        # 运行状态
        self.running = False
        self._fatal_shutdown = False
        self.initialized = False
        self.is_testnet = is_testnet
        self._start_balance_snap = None  # 启动余额快照（initialize 时设置）
        # 🌟 P0-2.2 + P1-5: 日净盈亏追踪 (UTC 00:00 重置)
        self._daily_loss_date = None      # 当前追踪的日期字符串 "YYYY-MM-DD"
        self._daily_realized_pnl = 0.0    # 今日已实现累计净盈亏 (USD, 负值表示亏损)
        self._daily_loss_triggered = False  # 今日是否已触发日损熔断
        self.daily_loss_limit_usd = 0.0   # 日损阈值 (0=关闭, main() 会从 config 读取)
        self.daily_loss_auto_close = False  # 触发时是否自动清仓
        # ============ 每日最大浮盈/浮亏持久化 ============
        self._drawdown_date = None
        self._drawdown_max_single_loss = 0.0
        self._drawdown_max_single_gain = 0.0
        self._drawdown_max_total_loss = 0.0
        self._drawdown_max_total_gain = 0.0
        self._drawdown_max_daily_net_loss = 0.0
        self._drawdown_last_persist_ts = 0.0
        _coin_label = config.BASE_CONFIG.get("target_currency", "BTC")
        _env_suffix = "testnet" if is_testnet else "main"
        self._db_path = f"trading_{_coin_label}_{_env_suffix}.db"
        self._drawdown_db_path = self._db_path
        self._drawdown_store = db_store.DrawdownStore(self._db_path)
        self._account_equity_store = db_store.AccountEquityStore(self._db_path)
        self._account_equity_date = None
        self._trade_store = db_store.TradeStore(self._db_path)
        self._spread_store = db_store.SpreadStore(self._db_path)
        # ================= Redis与异步队列 =================
        _currency = config.BASE_CONFIG.get("target_currency", "BTC")
        _db_map = {
            (False, "BTC"): 0, (False, "ETH"): 1,
            (True, "BTC"):  2, (True, "ETH"):  3,
        }
        self._redis_db = _db_map.get((is_testnet, _currency), 0)
        self._env_label = f"{'测试网' if is_testnet else '实盘'}-{_currency}"
        self.redis = redis.Redis(host='localhost', port=6379, db=self._redis_db, decode_responses=True)
        self.trade_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._trade_queue_backpressure_ts = 0.0
        self.persist_task = None
        # ================= 跨交易所: Binance 期货客户端 =================
        self.binance_auth = None
        self.binance_ws = None
        self.binance_executor = None
        self.binance_fee_calc = None
        self.binance_matcher = None
        self.binance_connected = False
        self._binance_tasks = []
        self._bn_reserved_margin = Decimal('0')
        self._margin_shutdown_active = False
        self._hedge_auto_recover_running = False
        # Binance 对冲参数
        self.binance_hedge_order_type = "MARKET"
        self.binance_max_slippage_usd = Decimal('5.0')
        self.binance_max_funding_rate = Decimal('0.001')
        self.binance_close_twap_slices = 4
        self.binance_close_twap_interval_sec = 0.25
        self.settlement_twap_enabled = True
        self.settlement_twap_minutes = 30
        self.settlement_twap_slices = 30
        self.basis_monitor_hours = 3.0
        self.basis_early_trigger_usd = 300.0
        self.basis_deterioration_trigger_usd = 150.0
        self.binance_use_hedge_mode = True
        self.binance_strict_hedge_mode = False
        self.binance_dual_side_mode = False
        self._last_hedge_mode_check_ts = 0.0
        self._hedge_mode_check_interval = 10.0
        self._strict_hedge_alert_ts = 0.0
        self._bn_side_integrity_last_check = 0.0
        self._bn_side_integrity_interval = 5.0
        self._bn_side_integrity_alert_ts = 0.0
        self._bn_side_integrity_tolerance = Decimal('0.001')
        self._bn_hedge_missing_since: Dict[Tuple[str, Decimal], float] = {}
        self._bn_hedge_missing_alert_ts = 0.0
        self._bn_hedge_missing_grace_sec = 8.0
        self._bn_residual_recheck_last_ts = 0.0
        self._bn_residual_recheck_interval = 5.0
        self._bn_residual_pause_log_ts = 0.0
        self._bn_residual_pause_tg_ts = 0.0
        self._monitor_risk_on_stopped_log_ts = 0.0
        self._paused_integrity_check_ts = 0.0
        self.executing_state_timeout_sec = 120.0
        self.post_anchor_min_profit_usd = Decimal('12.0')
        self.rollback_ioc_aggressive_ticks = 100
        self.rollback_l2_watch_seconds = 3.0
        self.ghost_rest_watch_seconds = 6.0
        self._settlement_pause_seconds = 120.0
        self.settlement_hard_stop_guard = True
        self.settlement_hard_stop_grace_seconds = 1200.0
        self._settlement_hard_stop_guard_log_ts = 0.0
        self._settlement_hard_stop_guard_tg_ts = 0.0
        self.risk_alert_throttle_seconds = 300.0
        self._event_log_throttle_ts: Dict[str, float] = {}
        # 性能监控
        self.scan_count = 0
        self.opportunities_found = 0
        self.trades_executed = 0
        # 24h交易量过滤缓存
        self._active_options = None
        self.min_option_volume = 0
        self.maker_price_aggression = 0.8
        self._volume_refresh_time = 0
        self._volume_refresh_interval = 30 * 60
        self._instrument_refresh_time = 0
        self._instrument_refresh_interval = 3600
        self._fee_refresh_time = 0.0
        self._fee_refresh_last_attempt_time = 0.0
        self._fee_refresh_interval = 3600.0
        self._fee_refresh_retry_interval = 300.0
        self._fee_refresh_lock = asyncio.Lock()
        self._deribit_fee_source = f"config:{getattr(self.fee_calculator, 'tier', 'standard')}"
        # ================= 全局风控参数 =================
        self.global_max_delta = Decimal('0.15')
        self.global_hard_delta = Decimal('0.50')
        self.hard_stop_loss_usd = Decimal('300')
        self.max_net_gamma = Decimal('0.02')
        self.max_total_positions = 10
        self.max_positions_per_expiry = 3
        self.max_perpetual_hold_hours = 48
        self.post_fill_negative_action = "hold"
        self._spread_record_interval = 300
        self._spread_last_record = 0.0
        self.record_spread_snapshots = True
        self.maker_top5_log_interval_seconds = 300.0
        self._scan_maker_top_profit_samples = []
        self._scan_maker_top_profit_window_started = 0.0
        # Layer 3: 幽灵仓位宽限期追踪
        self._ghost_first_seen: Dict[str, float] = {}
        self._ghost_closing: set = set()
        self._anchor_rollback_cleared_instruments: Set[str] = set()
        self._anchor_ws_disconnect_pending_orders: Set[str] = set()
        self._anchor_settlement_core_pending_orders: Set[str] = set()
        self._pending_stop_all_after_settlement = False
        self._anchor_ws_disconnect_log_ts = 0.0
        self._anchor_ws_disconnect_alert_ts = 0.0
        self._bn_ghost_first_seen: Dict[str, float] = {}
        self._bn_ghost_handling: set = set()
        self._broken_combos_alerted: set = set()
        self._broken_combo_first_seen: Dict[Tuple[str, Decimal], float] = {}
        self._broken_combo_handling: set = set()
        self._broken_combo_retry_after: Dict[Tuple[str, Decimal], float] = {}
        self._bn_mark_missing_since: Dict[Tuple[str, Decimal], float] = {}
        self._bn_mark_degraded_log_ts: Dict[Tuple[str, Decimal], float] = {}
        self._combo_fail_count: Dict[Tuple[str, Decimal], int] = {}
        self._combo_cooldown_until: Dict[Tuple[str, Decimal], float] = {}
        self.max_funding_rate_pct = Decimal('0.05')
        self.min_depth_ratio = Decimal('0.5')

    # ================= 暂停状态管理方法 =================
    @property
    def trading_paused(self) -> bool:
        """集合非空即暂停"""
        return len(self._pause_reasons) > 0

    @property
    def _pause_reason(self) -> str:
        """返回所有活跃暂停原因的可读字符串"""
        if not self._pause_reasons:
            return ""
        return " | ".join(sorted(self._pause_reasons))

    @_pause_reason.setter
    def _pause_reason(self, value: str):
        """兼容旧代码：直接赋值 reason 时忽略（原因已由 _add_pause 管理）"""
        pass

    @property
    def _paused_by_network(self) -> bool:
        """兼容旧代码：检查是否有网络类暂停原因"""
        return "Deribit WS断连" in self._pause_reasons or "Binance WS断连" in self._pause_reasons

    @_paused_by_network.setter
    def _paused_by_network(self, value: bool):
        """兼容旧代码：设置/清除网络暂停"""
        if not value:
            self._pause_reasons.discard("Deribit WS断连")
            self._pause_reasons.discard("Binance WS断连")

    def _add_pause(self, reason: str):
        """添加暂停原因"""
        self._pause_reasons.add(reason)

    def _remove_pause(self, reason: str):
        """移除暂停原因（其他原因仍保持暂停）"""
        self._pause_reasons.discard(reason)

    def _has_pause(self, reason: str) -> bool:
        """检查特定原因是否活跃"""
        return reason in self._pause_reasons

    def _should_emit_throttled(self, key: str, interval: Optional[float] = None) -> bool:
        """Return True when a repeated operational event should be logged again."""
        try:
            gap = float(interval if interval is not None else self.risk_alert_throttle_seconds)
        except Exception:
            gap = 300.0
        gap = max(gap, 0.0)
        now = time.time()
        last = self._event_log_throttle_ts.get(key, 0.0)
        if now - last >= gap:
            self._event_log_throttle_ts[key] = now
            return True
        return False

    def _is_deribit_settlement_core_window(self, at_utc=None) -> bool:
        """Deribit 08:00 UTC core settlement window.

        Core window is the exchange lock-risk period. During this window the
        engine may cancel/read/log, but must not start automated Deribit
        cleanup or liquidation orders.
        """
        try:
            from datetime import datetime, timezone

            utc_now = at_utc or datetime.now(timezone.utc)
            if utc_now.tzinfo is None:
                utc_now = utc_now.replace(tzinfo=timezone.utc)
            else:
                utc_now = utc_now.astimezone(timezone.utc)
            settle_at = utc_now.replace(hour=8, minute=0, second=0, microsecond=0)
            window_sec = float(getattr(self, '_settlement_pause_seconds', 120.0))
            return abs((utc_now - settle_at).total_seconds()) <= window_sec
        except Exception:
            return False

    def _is_settlement_risk_grace_window(self, at_utc=None) -> bool:
        """Deribit settlement risk guard window: core + post-settlement grace."""
        try:
            from datetime import datetime, timezone

            utc_now = at_utc or datetime.now(timezone.utc)
            if utc_now.tzinfo is None:
                utc_now = utc_now.replace(tzinfo=timezone.utc)
            else:
                utc_now = utc_now.astimezone(timezone.utc)
            settle_at = utc_now.replace(hour=8, minute=0, second=0, microsecond=0)
            core_window = float(getattr(self, '_settlement_pause_seconds', 120.0))
            grace_window = max(float(getattr(self, 'settlement_hard_stop_grace_seconds', 1200.0)), 0.0)
            return abs((utc_now - settle_at).total_seconds()) <= (core_window + grace_window)
        except Exception:
            return False

    def _on_binance_market_disconnect(self, reason: str = "") -> None:
        """Binance 市场 WS 断线即时回调：先 fail closed，再等 run loop 做恢复。"""
        _first = not self._has_pause("Binance WS断连")
        self.binance_connected = False
        self._add_pause("Binance WS断连")
        if _first:
            logger.warning(f"⚠️ Binance 市场数据断开，已立即暂停开新仓 ({reason})")
            try:
                asyncio.create_task(tg_notifier.send_async(
                    "⚠️ 【Binance 断开】市场数据通道中断，已立即暂停开新仓"))
            except RuntimeError:
                pass

    def _binance_market_ready(
            self, symbol: str, max_age_sec: float = 5.0,
            orderbook_max_age_sec: float = None,
            mark_max_age_sec: float = None,
            last_max_age_sec: float = None) -> Tuple[bool, str]:
        """开仓前 Binance 最终门禁：连接、盘口、mark、last 必须同时新鲜。

        max_age_sec 保留为兼容参数；未显式传入分项阈值时使用它。
        """
        ob_age_limit = float(orderbook_max_age_sec if orderbook_max_age_sec is not None else max_age_sec)
        mark_age_limit = float(mark_max_age_sec if mark_max_age_sec is not None else max_age_sec)
        last_age_limit = float(last_max_age_sec if last_max_age_sec is not None else max_age_sec)
        if not symbol:
            return False, "missing_symbol"
        if self.binance_ws is None:
            return False, "missing_ws"
        if not self.binance_connected:
            return False, "engine_disconnected"
        try:
            if not self.binance_ws.connected:
                return False, "ws_disconnected"
        except Exception:
            return False, "ws_state_unknown"

        now = time.time()
        ob = self.binance_ws.order_books.get(symbol)
        if not ob:
            return False, "orderbook_missing"
        if ob.mid_price is None or ob.mid_price <= 0:
            return False, "orderbook_mid_invalid"
        if not ob.best_bid or not ob.best_ask or ob.best_bid <= 0 or ob.best_ask <= 0:
            return False, "orderbook_bidask_invalid"
        if not ob.update_time or (now - ob.update_time) > ob_age_limit:
            return False, f"orderbook_stale:{(now - ob.update_time) if ob.update_time else -1:.1f}s"

        mark_price = self.binance_ws.mark_prices.get(symbol, Decimal("0"))
        mark_ts = self.binance_ws.mark_price_update_times.get(symbol, 0.0)
        if mark_price <= 0:
            return False, "mark_missing"
        if not mark_ts or (now - mark_ts) > mark_age_limit:
            return False, f"mark_stale:{(now - mark_ts) if mark_ts else -1:.1f}s"

        last_price = self.binance_ws.last_prices.get(symbol, Decimal("0"))
        last_ts = self.binance_ws.last_price_update_times.get(symbol, 0.0)
        if last_price <= 0:
            return False, "last_missing"
        if not last_ts or (now - last_ts) > last_age_limit:
            return False, f"last_stale:{(now - last_ts) if last_ts else -1:.1f}s"

        return True, "ok"

    _get_dynamic_tick = staticmethod(EnhancedDeribitWebSocketClient._get_dynamic_tick)
