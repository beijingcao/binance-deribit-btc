"""engine/settlement_mixin.py — 交割结算 + 紧急平仓 + Binance 对冲关闭 + TWAP"""
from __future__ import annotations
import logging
import time
import asyncio
import traceback
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Set, Any

if TYPE_CHECKING:
    pass

import aiohttp
import binance_futures
from telegram_handler import tg_notifier
from engine.models import ArbitrageState, Position, Order

logger = logging.getLogger(__name__)


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        if value is None:
            return Decimal('0')
        dec = Decimal(str(value))
        if not dec.is_finite():
            return Decimal('0')
        return dec
    except Exception:
        return Decimal('0')


def _extract_binance_fill_avg(
    order_result: Optional[dict],
    filled_qty: Decimal,
    fallback_avg: Decimal = Decimal('0'),
) -> Decimal:
    """Extract a usable Binance fill average.

    Binance USD-M can return FILLED with avgPrice=0. In that case cumQuote /
    executedQty is the exact average when available. If Binance omits both, use
    the previous slice average as a bounded fallback instead of poisoning VWAP.
    """
    if not isinstance(order_result, dict):
        return fallback_avg if fallback_avg > 0 else Decimal('0')

    for key in ('avgPrice', 'average_price'):
        avg = _decimal_or_zero(order_result.get(key))
        if avg > 0:
            return avg

    executed = _decimal_or_zero(order_result.get('executedQty'))
    if executed <= 0:
        executed = filled_qty

    if executed > 0:
        for key in ('cumQuote', 'cummulativeQuoteQty', 'quoteQty'):
            quote = _decimal_or_zero(order_result.get(key))
            if quote > 0:
                return quote / executed

    return fallback_avg if fallback_avg > 0 else Decimal('0')


def _calculate_twap_vwap(
    notional_total: Decimal,
    filled_total: Decimal,
    priced_filled_total: Optional[Decimal] = None,
    last_avg: Decimal = Decimal('0'),
) -> Decimal:
    """Calculate TWAP VWAP without dividing priced notional by unpriced fills."""
    priced_qty = priced_filled_total if priced_filled_total is not None else filled_total
    if notional_total > 0:
        denom = priced_qty if priced_qty and priced_qty > 0 else filled_total
        if denom and denom > 0:
            return notional_total / denom
    return last_avg if last_avg > 0 else Decimal('0')


class SettlementMixin:
    """Mixin: 交割结算 + 紧急平仓 + Binance 对冲关闭 + Settlement TWAP"""

    async def _check_settlement_window(self):
        """每日结算窗口规避：Deribit 08:00 UTC，前后各配置秒数暂停交易"""
        from datetime import datetime, timezone
        utc_now = datetime.now(timezone.utc)
        window_sec = float(getattr(self, '_settlement_pause_seconds', 60.0))
        in_window = self._is_deribit_settlement_core_window(utc_now)

        if in_window:
            if not getattr(self, '_settlement_paused', False):
                self._settlement_paused = True
                logger.warning(
                    f"⏸️ [结算规避] 进入 Deribit 每日结算窗口 (08:00 UTC ±{int(window_sec)}s)，"
                    f"暂停交易并撤销所有挂单")
                # 撤掉所有挂单，防止结算期间被意外成交
                try:
                    await self.client.cancel_all_orders(self.target_currency)
                except Exception as e:
                    logger.error(f"[结算规避] 撤单异常: {e}")
                    asyncio.create_task(tg_notifier.send_error_async(f"结算规避撤单异常: {e}", "settlement_cancel_error"))
                self._add_pause("结算窗口")
        else:
            if getattr(self, '_settlement_paused', False):
                self._settlement_paused = False
                self._remove_pause("结算窗口")
                logger.info(f"▶️ [结算规避] 结算窗口结束"
                            f"{'，但其他暂停原因仍有效' if self.trading_paused else '，系统自动恢复交易'}")
            if getattr(self, '_pending_stop_all_after_settlement', False):
                self._pending_stop_all_after_settlement = False
                logger.warning("▶️ [结算规避] core window 结束，恢复执行延后的 /stop_all 清仓")
                try:
                    await self.emergency_liquidate_all(full_stop=True)
                except Exception as e:
                    self._pending_stop_all_after_settlement = True
                    logger.error(f"[结算规避] 恢复延后 /stop_all 失败: {e}")

    async def _emergency_dump_all(self, state: ArbitrageState, combination: Dict, f_pos: Position, c_pos: Position,
                                  p_pos: Position):
        """🚨 灾难强平：无视流动性与滑点成本，强制清空特定组合的所有仓位"""
        # 🌟 B 修复: 以 per-combo 锁包裹, 与 _handle_delivery_settlement 串行
        _closing_lock = self._get_closing_lock(state)
        async with _closing_lock:
            return await self._emergency_dump_all_locked(state, combination, f_pos, c_pos, p_pos)

    async def _emergency_dump_all_locked(self, state: ArbitrageState, combination: Dict, f_pos: Position, c_pos: Position,
                                          p_pos: Position):
        """原 _emergency_dump_all 主体, 通过锁包裹后的内部实现"""
        if self._is_deribit_settlement_core_window():
            self._add_pause("结算窗口")
            _now = time.time()
            if _now - getattr(self, '_settlement_core_freeze_log_ts', 0.0) >= 30:
                self._settlement_core_freeze_log_ts = _now
                logger.warning(
                    f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] ⏸️ Deribit core settlement window active; "
                    f"跳过紧急强平，等待窗口结束后重新评估")
            return
        # 🌟 B 修复: 进入临界区后二次校验状态 — 若已被 settlement 清理, 跳过
        if state.state in ('exited', 'closed', 'cleaned'):
            logger.info(f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] 紧急强平入口: "
                        f"仓位已被其他路径清理 (state={state.state}), 跳过")
            return
        state.state = 'exiting'
        log_prefix = f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}-紧急逃生]"
        if self._should_emit_throttled(f"emergency_dump_start:{state.expiry_strike}"):
            logger.error(f"{log_prefix} 正在执行断尾求生...")
        _bn_result = None

        # 取消进行中的结算TWAP任务并等待终止，避免并发平仓竞争
        _twap_task = getattr(state, '_settlement_twap_task', None)
        if _twap_task and not _twap_task.done():
            _twap_task.cancel()
            try:
                await _twap_task
            except (asyncio.CancelledError, Exception):
                pass
            logger.info(f"{log_prefix} 已取消结算TWAP任务")

        # ================= 核心修复：使用 state 记录的 per-combo 数量，而非全局持仓 =================
        combo_future_usd = getattr(state, 'future_size_usd', Decimal('0'))
        combo_opt_amount = getattr(state, 'entry_amount', Decimal('0'))

        # 兜底：如果 state 没有记录(老状态兼容)，用 position 数量但记录 warning
        if combo_future_usd == Decimal('0') and f_pos and f_pos.size != 0:
            combo_future_usd = abs(f_pos.size)
            logger.warning(f"{log_prefix} state.future_size_usd 为空，降级使用全局持仓 {combo_future_usd}")
        if combo_opt_amount == Decimal('0') and c_pos and c_pos.size != 0:
            combo_opt_amount = abs(c_pos.size)
            logger.warning(f"{log_prefix} state.entry_amount 为空，降级使用全局持仓 {combo_opt_amount}")

        # 根据 strategy_type 决定方向
        strategy = getattr(state, 'strategy_type', '')
        if not strategy:
            # 从持仓推断 (兼容老状态)
            if c_pos and c_pos.size > 0:
                strategy = 'sell_future_buy_synthetic'
            else:
                strategy = 'buy_future_sell_synthetic'
            logger.warning(f"{log_prefix} state.strategy_type 为空，从持仓推断: {strategy}")

        if strategy == 'sell_future_buy_synthetic':
            # 空期货 + 多Call + 空Put → 平仓: 买期货 + 卖Call + 买Put
            f_side, c_side, p_side = 'buy', 'sell', 'buy'
        else:
            # 多期货 + 空Call + 多Put → 平仓: 卖期货 + 买Call + 卖Put
            f_side, c_side, p_side = 'sell', 'buy', 'sell'

        # 0. 🌟 跨所: 先强平 Binance 对冲腿 (优先级最高，防止裸腿敞口)
        _bn_qty_snapshot = state.binance_filled_qty  # 保存快照，close 后会清零
        _bn_entry_snapshot = state.binance_entry_price or Decimal('0')
        _bn_dump_ok = False
        if state.binance_future_symbol and state.binance_filled_qty > 0:
            logger.error(f"{log_prefix} 正在强平 Binance 对冲: {state.binance_future_symbol} × {state.binance_filled_qty}")
            _bn_result = await self._close_binance_hedge(state, emergency=True)
            # 🌟 2026-04-17 回归修复 (emergency TWAP 1→3 片副作用):
            #   原代码只检查 _bn_result 非 None, 但 3 片 TWAP 可能部分成交返回
            #   {status:FILLED, executedQty:0.033} 非 None → 被误判为完全成功
            #   → 继续平 Deribit 期权 → Binance 残留 0.067 BTC 裸腿
            #   修复: 同时检查 state.binance_filled_qty 是否已清零 (_apply_combo_fill 会扣减)
            #   与 _handle_delivery_settlement_locked line 10283 的判定口径一致
            _residual_dust = Decimal('0.0001')
            if _bn_result and state.binance_filled_qty <= _residual_dust:
                _bn_dump_ok = True
                # 🌟 P2 回归修复: 紧急强平也要保存 close_order_ids 供审计
                self._capture_binance_close_ids(state, _bn_result)
                # 🌟 2026-04-24: 打标对冲关闭时刻，供 '实际对冲关闭时间' 字段使用
                state._hedge_close_completed_ts = time.time()
                logger.info(f"{log_prefix} ✅ Binance 对冲已强平: 均价={_bn_result.get('avgPrice', '?')}")
            elif _bn_result:
                # 部分成交: 保留 close IDs 但视作未完成, 不平 Deribit 防裸腿
                self._capture_binance_close_ids(state, _bn_result)
                logger.error(
                    f"{log_prefix} 🚨 Binance 部分强平: 已平 {_bn_qty_snapshot - state.binance_filled_qty}, "
                    f"残余 {state.binance_filled_qty} — 下轮重试, 暂不平 Deribit 防裸腿")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"{log_prefix} Binance 部分强平, 残余 {state.binance_filled_qty} {state.binance_future_symbol}\n"
                    f"可能被 -4131 PERCENT_PRICE filter 拒绝部分片, 将在下轮重试",
                    "emergency_dump_partial_fill"))
            else:
                logger.error(f"{log_prefix} 🚨 Binance 强平失败！将在下次循环重试")
                asyncio.create_task(tg_notifier.send_error_async(
                    f"{log_prefix} Binance 对冲强平失败！请立即检查 {state.binance_future_symbol} 持仓",
                    "emergency_dump_binance_fail"))
        else:
            _bn_dump_ok = True  # 无 Binance 腿，视为成功

        # 🌟 P0 修复: Binance 平仓失败时，不继续平 Deribit
        # 原因: 如果继续平 Deribit → Deribit 全清 + Binance 仍在 = 裸腿
        # 正确做法: 保留完整三腿对冲，等 Binance 恢复后再重试
        if not _bn_dump_ok and state.binance_future_symbol:
            logger.error(
                f"{log_prefix} 🛡️ Binance 对冲未平，为防裸腿，暂不平 Deribit 端。"
                f"保留完整对冲，等下轮重试。")
            state.state = 'position_open'
            state.last_update = time.time()
            await self._save_state_to_redis(state)
            return

        # 1. 强平期货 (流动性好，直接市价；下单前校验持仓方向)
        f_result = None
        _expected_f_close = Decimal('0')
        if combo_future_usd > 0 and f_pos and f_pos.size != 0:
            actual_f_dir = 'sell' if f_pos.size > 0 else 'buy'
            if actual_f_dir != f_side:
                # 持仓方向与预期一致才能 reduce_only 平仓
                logger.warning(f"{log_prefix} 期货持仓方向({actual_f_dir})与预期平仓方向({f_side})不一致，"
                              f"可能已被其他组合平掉，跳过期货腿")
            else:
                # 使用 min(state记录量, 实际持仓量) 防止超额平仓
                actual_f_amount = min(combo_future_usd, abs(f_pos.size))
                _expected_f_close = actual_f_amount
                f_result = await self.client.place_order(
                    combination['future'], actual_f_amount, f_side, 'market', reduce_only=True,
                    label=f"em_f", log_prefix=log_prefix)

        # 2. 强平 Call (使用 close_position API，避免 price_too_low/price_too_high 被交易所拒绝)
        c_result = None
        _expected_c_close = Decimal('0')
        if combo_opt_amount > 0 and c_pos and c_pos.size != 0:
            _expected_c_close = abs(c_pos.size)
            c_result = self._find_active_deribit_close_order(combination['call'])
            if c_result:
                if self._should_emit_throttled(f"emergency_pending_close:{combination['call']}", 60):
                    logger.warning(
                        f"{log_prefix} {combination['call']} 已有平仓挂单 {c_result.order_id}，"
                        f"等待成交/撤单，不重复发送 reduce-only")
            else:
                c_result = await self._close_option_position(combination['call'], log_prefix)

        # 3. 强平 Put (使用 close_position API)
        p_result = None
        _expected_p_close = Decimal('0')
        if combo_opt_amount > 0 and p_pos and p_pos.size != 0:
            _expected_p_close = abs(p_pos.size)
            p_result = self._find_active_deribit_close_order(combination['put'])
            if p_result:
                if self._should_emit_throttled(f"emergency_pending_close:{combination['put']}", 60):
                    logger.warning(
                        f"{log_prefix} {combination['put']} 已有平仓挂单 {p_result.order_id}，"
                        f"等待成交/撤单，不重复发送 reduce-only")
            else:
                p_result = await self._close_option_position(combination['put'], log_prefix)

        # ================= 验证成交结果，未完全平仓不标记 exited =================
        def _is_leg_closed(order_obj, expected_qty: Decimal, tolerance: Decimal = Decimal('0')) -> bool:
            if expected_qty <= tolerance:
                return True
            if not order_obj:
                return False
            try:
                if isinstance(order_obj, dict):
                    _filled = Decimal(str(
                        order_obj.get('filled_amount',
                                      order_obj.get('executedQty', order_obj.get('filled_qty', 0)))))
                    _status = str(order_obj.get('order_state', order_obj.get('status', ''))).lower()
                else:
                    _filled = Decimal(str(getattr(order_obj, 'filled_amount', Decimal('0'))))
                    _status = str(getattr(order_obj, 'status', '')).lower()
                if _status in ('filled', 'closed'):
                    return True
                return _filled >= max(expected_qty - tolerance, Decimal('0'))
            except Exception:
                return False

        def _extract_fill_price(order_obj) -> Decimal:
            if not order_obj:
                return Decimal('0')
            try:
                if isinstance(order_obj, dict):
                    for _k in ('avgPrice', 'average_price', 'price'):
                        _v = order_obj.get(_k, 0)
                        _d = Decimal(str(_v))
                        if _d > 0:
                            return _d
                    return Decimal('0')
                _avg = Decimal(str(getattr(order_obj, 'average_price', 0)))
                if _avg > 0:
                    return _avg
                _px = Decimal(str(getattr(order_obj, 'price', 0)))
                return _px if _px > 0 else Decimal('0')
            except Exception:
                return Decimal('0')

        dump_failures = []
        if not _bn_dump_ok:
            dump_failures.append(f"Binance {state.binance_future_symbol}")
        if _expected_f_close > 0:
            if not _is_leg_closed(f_result, _expected_f_close, Decimal('1')):
                dump_failures.append(f"期货 {combination['future']}")
        if _expected_c_close > 0:
            if not _is_leg_closed(c_result, _expected_c_close, Decimal('0.0001')):
                dump_failures.append(f"Call {combination['call']}")
        if _expected_p_close > 0:
            if not _is_leg_closed(p_result, _expected_p_close, Decimal('0.0001')):
                dump_failures.append(f"Put {combination['put']}")

        if dump_failures:
            failure_detail = ", ".join(dump_failures)
            if self._should_emit_throttled(f"emergency_dump_partial:{state.expiry_strike}:{failure_detail}"):
                logger.error(f"{log_prefix} 🚨 紧急逃生部分失败！未平仓腿: {failure_detail}")
            asyncio.create_task(tg_notifier.send_error_async(
                f"{log_prefix} 紧急逃生部分失败！\n未平仓腿: {failure_detail}\n"
                f"⚠️ state 保持 position_open，系统将持续重试。请检查交易所持仓！",
                "emergency_dump_partial"))
            # 不标记 exited，不删 Redis，让系统持续监控并重试
            state.state = 'position_open'
            state.last_update = time.time()
            await self._save_state_to_redis(state)
        else:
            # ================= 紧急强平记录写入（含 P&L 估算）=================
            expiry, strike = state.expiry_strike
            _em_amount = state.entry_amount or self.trade_amount
            _em_entry = state.entry_prices or {}
            _em_f_entry = _em_entry.get('future', Decimal('0'))
            _em_c_entry = _em_entry.get('call', Decimal('0'))
            _em_p_entry = _em_entry.get('put', Decimal('0'))

            # 平仓成交价：Binance 从 _bn_result，期权从 close_position 结果
            _em_f_exit = Decimal(str(_bn_result.get('avgPrice', 0))) if _bn_dump_ok and _bn_result else (
                Decimal(str(f_result.average_price)) if f_result and hasattr(f_result, 'average_price') and f_result.average_price > 0 else Decimal('0'))
            _em_c_exit = _extract_fill_price(c_result)
            _em_p_exit = _extract_fill_price(p_result)

            # 估算 P&L
            _em_pnl = 0.0
            _em_fee = 0.0
            try:
                _em_ref = _em_f_exit if _em_f_exit > 0 else _em_f_entry
                if _em_ref > 0 and _em_c_entry > 0 and _em_p_entry > 0:
                    # 期权盈亏 (BTC)
                    _em_c_diff = (_em_c_exit - _em_c_entry) if _em_c_exit > 0 else Decimal('0')
                    _em_p_diff = (_em_p_exit - _em_p_entry) if _em_p_exit > 0 else Decimal('0')
                    if strategy == 'sell_future_buy_synthetic':
                        _em_opt_pnl = (_em_c_diff - _em_p_diff) * _em_amount
                    else:
                        _em_opt_pnl = (-_em_c_diff + _em_p_diff) * _em_amount
                    _em_pnl += float(_em_opt_pnl * _em_ref)
                    # Binance 期货盈亏
                    if _em_f_exit > 0 and _bn_entry_snapshot > 0 and _bn_qty_snapshot > 0:
                        if strategy == 'sell_future_buy_synthetic':
                            _em_pnl += float((_bn_entry_snapshot - _em_f_exit) * _bn_qty_snapshot)
                        else:
                            _em_pnl += float((_em_f_exit - _bn_entry_snapshot) * _bn_qty_snapshot)
                    # 手续费估算：改为动态费率（Deribit/BN 均读取当前同步值）
                    _em_open_fee_usd = Decimal('0')
                    _em_close_fee_usd = Decimal('0')
                    _em_bn_qty = _bn_qty_snapshot if _bn_qty_snapshot > 0 else _em_amount
                    _fc = self.trade_executor.fee_calculator

                    # 开仓费（按 entry 快照）
                    if _em_f_entry > 0 and _em_c_entry > 0 and _em_p_entry > 0:
                        _em_oc_btc = _fc.calculate_option_fee(
                            _em_f_entry, _em_c_entry, _em_amount, is_taker=True)
                        _em_op_btc = _fc.calculate_option_fee(
                            _em_f_entry, _em_p_entry, _em_amount, is_taker=True)
                        _em_open_fee_usd += (_em_oc_btc + _em_op_btc) * _em_f_entry
                    if _bn_entry_snapshot > 0 and _em_bn_qty > 0:
                        _em_open_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                            _bn_entry_snapshot, _em_bn_qty, is_taker=True)

                    # 平仓费（按实际成交价，缺失时降级 entry）
                    _em_c_close_ref = _em_c_exit if _em_c_exit > 0 else _em_c_entry
                    _em_p_close_ref = _em_p_exit if _em_p_exit > 0 else _em_p_entry
                    _em_f_close_ref = _em_f_exit if _em_f_exit > 0 else _em_ref
                    if _em_f_close_ref > 0 and _em_c_close_ref > 0 and _em_p_close_ref > 0:
                        _em_cc_btc = _fc.calculate_option_fee(
                            _em_f_close_ref, _em_c_close_ref, _em_amount, is_taker=True)
                        _em_cp_btc = _fc.calculate_option_fee(
                            _em_f_close_ref, _em_p_close_ref, _em_amount, is_taker=True)
                        _em_close_fee_usd += (_em_cc_btc + _em_cp_btc) * _em_f_close_ref
                    if _em_f_close_ref > 0 and _em_bn_qty > 0:
                        _em_close_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                            _em_f_close_ref, _em_bn_qty, is_taker=True)

                    _em_fee = float(_em_open_fee_usd + _em_close_fee_usd)
                    _em_pnl -= _em_fee
            except Exception as _em_err:
                logger.info(f"{log_prefix} 紧急强平 P&L 估算异常: {_em_err}")

            emergency_record = {
                '订单ID': state.combo_id,
                '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                '策略方向': state.strategy_type or '', '到期日': expiry, '行权价': float(strike),
                '标的': self.target_currency,
                '期权数量': float(_em_amount),
                '期货面值(USD)': float(state.future_size_usd) if state.future_size_usd else 0,
                '模拟_Future价格': float(_em_f_entry), '实际_Future均价': float(_em_f_exit),
                '模拟_Call价格': float(_em_c_entry), '实际_Call均价': float(_em_c_exit),
                '模拟_Put价格': float(_em_p_entry), '实际_Put均价': float(_em_p_exit),
                '模拟_手续费(USD)': 0, '实际_手续费(USD)': round(_em_fee, 4),
                '开仓手续费(USD)': '',
                '预估结算手续费(USD)': '',
                '已实现funding(USD)': 0.0,
                '模拟_净利润(USD)': 0, '实际_净利润(USD)': round(_em_pnl, 4),
                '滑点与偏差损失(USD)': 0,
                # 🌟 P1-17: 紧急强平补充订单 ID (用于事后复盘)
                'Call_ID': getattr(state, 'call_order_id', '') or 'UNCONFIRMED',
                'Put_ID': getattr(state, 'put_order_id', '') or 'UNCONFIRMED',
                'Future_ID': self._format_future_id(state),
                '交易类型': '紧急强平', '平仓原因': '硬止损',
                # 紧急强平是同步关闭，写入时间 ≈ 对冲关闭时间
                '实际对冲关闭时间': time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(float(getattr(state, '_hedge_close_completed_ts', 0.0)) or time.time())),
            }
            _inserted = await self._persist_terminal_record(emergency_record)
            if _inserted:
                try:
                    from datetime import datetime, timezone
                    _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    if getattr(self, '_daily_loss_date', None) != _today:
                        self._daily_loss_date = _today
                        self._daily_realized_pnl = 0.0
                        self._daily_loss_triggered = False
                    self._daily_realized_pnl += float(_em_pnl)
                    logger.info(f"📅 [日损追踪] 紧急强平累加, 今日净盈亏: ${self._daily_realized_pnl:+.2f}")
                    asyncio.create_task(self._save_daily_pnl_to_redis())
                except Exception:
                    pass

            asyncio.create_task(tg_notifier.send_error_async(
                f"{log_prefix} 已触及底层风控红线，执行了无条件市价/极速逃生强平！",
                "emergency_dump"))
            state.state = 'exited'
            state.last_update = time.time()
            self.position_locks.discard(state.expiry_strike)
            self._bn_mark_missing_since.pop(state.expiry_strike, None)
            self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
            self._broken_combo_first_seen.pop(state.expiry_strike, None)
            self._broken_combo_handling.discard(state.expiry_strike)
            self._combo_closing_locks.pop(state.expiry_strike, None)
            if hasattr(self, '_exit_attempt_notified'):
                self._exit_attempt_notified.discard(state.expiry_strike)
            if hasattr(self, '_exit_fail_notified'):
                self._exit_fail_notified.discard(state.expiry_strike)
            await self._delete_state_from_redis(state.expiry_strike[0], state.expiry_strike[1])
            asyncio.create_task(self._try_auto_resume_after_delivery(log_prefix))

    def _find_active_deribit_close_order(self, instrument_name: str) -> Optional[Order]:
        """Return an in-flight emergency/cleanup close order for this instrument, if any."""
        terminal = {'filled', 'cancelled', 'rejected'}
        close_prefixes = ('em_', 'ghost_', 'l2a_', 'l2t_', 'l2f_')
        for order in list(getattr(self.client, 'active_orders', {}).values()):
            try:
                if order.instrument_name != instrument_name:
                    continue
                if str(order.status or '').lower() in terminal:
                    continue
                label = str(order.label or '').lower()
                if label.startswith(close_prefixes) or label in ('em_close', 'em_opt'):
                    return order
            except Exception:
                continue
        return None

    def _synthetic_closed_order(self, instrument_name: str) -> Order:
        """Build a local closed marker when REST confirms no position remains."""
        return Order(
            order_id=f"no_position:{instrument_name}:{int(time.time() * 1000)}",
            instrument_name=instrument_name,
            side='',
            amount=Decimal('0'),
            price=Decimal('0'),
            order_type='synthetic',
            label='em_close_no_position',
            status='filled',
            timestamp=time.time(),
            filled_amount=Decimal('0'),
            average_price=Decimal('0')
        )

    async def _close_option_position(self, instrument_name: str, log_prefix: str = ""):
        """使用 Deribit close_position API 平仓期权，避免 price_too_low/high 被交易所拒绝"""
        try:
            logger.info(f"{log_prefix} 期权 close_position: {instrument_name}")
            close_resp = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "private/close_position",
                "params": {
                    "instrument_name": instrument_name,
                    "type": "market"
                }
            }, is_private=True)
            if 'error' in close_resp:
                err_msg = close_resp['error'].get('message', '未知')
                if self._should_emit_throttled(f"close_position_error:{instrument_name}:{err_msg}"):
                    logger.error(f"{log_prefix} close_position 失败: {instrument_name} -> {err_msg}")
                err_lower = str(err_msg).lower()
                if 'invalid_reduce_only_order' in err_lower:
                    positions_refreshed = False
                    try:
                        await self.client.get_positions(self.target_currency, silent=True)
                        positions_refreshed = True
                    except Exception as _refresh_err:
                        logger.warning(
                            f"{log_prefix} close_position 返回 invalid_reduce_only_order，"
                            f"但刷新持仓失败: {_refresh_err}；保持未完成状态等待下轮确认")
                    pending_order = self._find_active_deribit_close_order(instrument_name)
                    if pending_order:
                        if self._should_emit_throttled(f"close_position_pending:{instrument_name}", 60):
                            logger.warning(
                                f"{log_prefix} {instrument_name} 已有平仓挂单 {pending_order.order_id}，"
                                f"不重复发送 reduce-only")
                        return pending_order
                    if positions_refreshed:
                        pos_after = self.client.positions.get(instrument_name)
                        if not pos_after or pos_after.size == 0:
                            if self._should_emit_throttled(f"close_position_no_position:{instrument_name}", 60):
                                logger.info(f"{log_prefix} {instrument_name} 已无持仓，视为平仓完成")
                            return self._synthetic_closed_order(instrument_name)
                    else:
                        return None
                # 降级方案：用交易所允许范围内的极端价 IOC 限价单，确保立即成交
                ticker = self.client.tickers.get(instrument_name)
                pos = self.client.positions.get(instrument_name)
                if ticker and pos and pos.size != 0:
                    side = 'sell' if pos.size > 0 else 'buy'
                    if side == 'sell':
                        # 卖出：使用交易所允许的最低价（min_price），保证立即成交
                        if ticker.min_price > 0:
                            px = ticker.min_price
                        else:
                            px = max(ticker.bid * Decimal('0.5'), Decimal('0.0001'))
                    else:
                        # 买入：使用交易所允许的最高价（max_price），保证立即成交
                        if ticker.max_price > 0:
                            px = ticker.max_price
                        else:
                            px = ticker.ask * Decimal('2') if ticker.ask > 0 else Decimal('1.0')
                    px = self.client._adjust_to_tick_size(px, self._get_dynamic_tick(px))
                    result = await self.client.place_order(
                        instrument_name, abs(pos.size), side, 'limit', price=px,
                        reduce_only=True, time_in_force="immediate_or_cancel",
                        label="em_opt", log_prefix=log_prefix)
                    return result
                return None
            else:
                # close_position 成功，构造一个简易的成功标记
                order_data = close_resp.get('result', {}).get('order', {})
                logger.info(f"{log_prefix} close_position 成功: {instrument_name} | "
                           f"状态: {order_data.get('order_state', '?')} | "
                           f"成交: {order_data.get('filled_amount', '?')}")
                order = self.client._order_from_api_data(order_data)
                if not order.instrument_name:
                    order.instrument_name = instrument_name
                if not order.label:
                    order.label = 'em_close'
                order.order_type = 'market'
                if not order_data.get('order_state'):
                    order.status = 'filled'
                self.client._store_order_snapshot(order)
                return order
        except Exception as e:
            logger.error(f"{log_prefix} close_position 异常: {instrument_name} -> {e}")
            return None

    async def _close_binance_hedge(self, state: ArbitrageState, emergency: bool = False):
        """平仓 Binance 期货对冲腿（按组合数量部分平仓，非全仓平仓）

        平仓执行策略：
        - 优先走 Binance 执行器
        - 正常场景：短 TWAP 分片市价单（reduceOnly）降低一次性冲击
        - 紧急场景：单笔市价优先速度，避免风险暴露时间拉长
        - executor 不可用/未完全成交时，自动降级到 REST 分片继续平
        """
        if not state.binance_future_symbol:
            return None
        try:
            _symbol = state.binance_future_symbol
            _position_side = (state.binance_position_side or "").upper()
            if _position_side not in ("LONG", "SHORT") and self.binance_dual_side_mode:
                if state.strategy_type == 'buy_future_sell_synthetic':
                    _position_side = "LONG"
                elif state.strategy_type == 'sell_future_buy_synthetic':
                    _position_side = "SHORT"
            _dust = Decimal('0.0001')
            _combo_qty = state.binance_filled_qty if state.binance_filled_qty > 0 else Decimal('0')
            if getattr(state, 'binance_open_qty', Decimal('0')) <= 0 and _combo_qty > _dust:
                # 兼容历史状态：若缺少开仓数量快照，至少保证不低于当前持仓
                state.binance_open_qty = _combo_qty
            if _combo_qty <= _dust:
                state.binance_filled_qty = Decimal('0')
                return {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

            # 交易所持仓是按 symbol 汇总仓位（可能包含多个组合），这里只做上限保护，不直接覆写组合数量
            _actual_total_qty, _actual_side, _, _actual_known = await self._get_binance_actual_position(
                _symbol, _position_side)
            if not _actual_known:
                logger.warning(f"[Binance平仓对账] 无法确认交易所真实持仓({_symbol})，保留状态等待重试")
                return None
            if _actual_total_qty <= _dust:
                logger.warning(
                    f"[Binance平仓对账] 状态机记录 {_symbol} qty={_combo_qty}，但交易所实际为0，自动清零该组合")
                state.binance_filled_qty = Decimal('0')
                return {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

            _close_qty = min(_combo_qty, _actual_total_qty)
            if _close_qty <= _dust:
                return None

            # 根据策略方向确定平仓方向
            if state.strategy_type == 'buy_future_sell_synthetic':
                close_side = 'SELL'   # 开仓做多 → 平仓做空
            elif state.strategy_type == 'sell_future_buy_synthetic':
                close_side = 'BUY'    # 开仓做空 → 平仓做多
            elif _actual_side == 'LONG':
                close_side = 'SELL'
            elif _actual_side == 'SHORT':
                close_side = 'BUY'
            else:
                # 未知策略，从 Binance 持仓方向推断
                _bn_pos = None
                if self.binance_ws:
                    if _position_side in ("LONG", "SHORT"):
                        _bn_pos = self.binance_ws.positions_by_side.get((_symbol, _position_side))
                    if _bn_pos is None:
                        _bn_pos = self.binance_ws.positions.get(_symbol)
                if _bn_pos and _bn_pos.side == 'LONG':
                    close_side = 'SELL'
                elif _bn_pos and _bn_pos.side == 'SHORT':
                    close_side = 'BUY'
                else:
                    logger.error(f"无法确定 Binance 平仓方向: {_symbol}")
                    return None

            _twap_slices = max(int(getattr(self, 'binance_close_twap_slices', 4)), 1)
            _twap_slices = min(_twap_slices, 20)  # 安全上限，防止误配置拖慢平仓
            _twap_interval = max(float(getattr(self, 'binance_close_twap_interval_sec', 0.25)), 0.05)
            _twap_interval = min(_twap_interval, 2.0)
            if emergency:
                # 🌟 2026-04-17 改进: 原来 slices=1 一把梭容易触发 Binance -4131 PERCENT_PRICE filter
                # (单笔市价单吃穿薄盘口 → 偏离 markPrice 过多 → 被拒)
                # 改为 3 片 + 50ms 间隔: 比正常模式快, 但避免单次冲击过大
                _twap_slices = 3
                _twap_interval = 0.05
            _bn_info = getattr(self.binance_executor, 'contract_info', {}).get(_symbol, {}) if self.binance_executor else {}
            _bn_step = Decimal(str(_bn_info.get('step_size', '0.001') or '0.001'))
            if _bn_step <= 0:
                _bn_step = Decimal('0.001')
            _bn_min_qty = Decimal(str(_bn_info.get('min_qty', _bn_step) or _bn_step))
            if _bn_min_qty <= 0:
                _bn_min_qty = _bn_step

            _twap_filled_total = Decimal('0')
            _twap_notional_total = Decimal('0')
            _twap_priced_filled_total = Decimal('0')
            _twap_order_count = 0
            _twap_order_ids: List[str] = []  # 🌟 P2: 收集每个 TWAP 分片的 orderId 供审计
            _twap_last_avg = Decimal('0')

            def _round_qty(_qty: Decimal) -> Decimal:
                if _qty <= 0:
                    return Decimal('0')
                if self.binance_executor:
                    try:
                        return self.binance_executor._round_qty(_symbol, _qty)
                    except Exception:
                        pass
                if _bn_step > 0:
                    return (_qty // _bn_step) * _bn_step
                return _qty

            def _next_slice_qty(_remaining_qty: Decimal, _slice_idx: int) -> Decimal:
                if _remaining_qty <= _dust:
                    return Decimal('0')
                _left = max(_twap_slices - _slice_idx, 1)
                _target = _remaining_qty if _left <= 1 else (_remaining_qty / Decimal(str(_left)))
                _q = _round_qty(_target)
                if _q < _bn_min_qty:
                    _q = _round_qty(_remaining_qty)
                if _q > _remaining_qty:
                    _q = _round_qty(_remaining_qty)
                if _q < _bn_min_qty:
                    return Decimal('0')
                return _q

            def _apply_combo_fill(_result: dict, _request_qty: Decimal) -> Decimal:
                """按组合维度扣减已平数量（不混入交易所总仓位）"""
                _filled = Decimal(str(_result.get('executedQty', '0'))) if _result else Decimal('0')
                if _filled <= 0 and _result and _result.get('status') == 'FILLED':
                    _filled = _request_qty
                state.binance_filled_qty = max(state.binance_filled_qty - _filled, Decimal('0'))
                if state.binance_filled_qty <= _dust:
                    state.binance_filled_qty = Decimal('0')
                return _filled

            def _record_twap_fill(_result: dict, _request_qty: Decimal, _src: str) -> Decimal:
                nonlocal _twap_filled_total, _twap_notional_total, _twap_priced_filled_total, _twap_order_count, _twap_last_avg
                _filled = _apply_combo_fill(_result, _request_qty)
                _raw_avg = _decimal_or_zero(_result.get('avgPrice')) if isinstance(_result, dict) else Decimal('0')
                _avg = _extract_binance_fill_avg(_result, _filled, _twap_last_avg)
                if _filled > 0:
                    _twap_filled_total += _filled
                    _twap_order_count += 1
                    if _avg > 0:
                        if _raw_avg <= 0:
                            logger.warning(
                                f"[Binance平仓TWAP/{_src}] {_symbol} {close_side} "
                                f"avgPrice=0，已用成交额或上一片均价恢复为 {_avg}")
                        _twap_last_avg = _avg
                        _twap_notional_total += _avg * _filled
                        _twap_priced_filled_total += _filled
                    else:
                        logger.warning(
                            f"[Binance平仓TWAP/{_src}] {_symbol} {close_side} "
                            f"成交={_filled} 但均价缺失，本片不计入 VWAP 定价分母")
                    # 🌟 P2 审计: 收集分片订单 ID (用于交割记录 Future_ID)
                    _oid = _result.get('orderId') if isinstance(_result, dict) else None
                    if _oid:
                        _twap_order_ids.append(str(_oid))
                logger.info(
                    f"[Binance平仓TWAP/{_src}] {_symbol} {close_side} 请求={_request_qty} "
                    f"成交={_filled} 均价={_avg if _avg > 0 else Decimal('0')}")
                return _filled

            def _build_twap_result(_status: str) -> Optional[dict]:
                if _twap_filled_total <= 0:
                    return None
                _avg = _calculate_twap_vwap(
                    _twap_notional_total, _twap_filled_total, _twap_priced_filled_total, _twap_last_avg)
                if _avg <= 0:
                    _avg = Decimal('0')
                return {
                    'status': _status,
                    'avgPrice': str(_avg),
                    'executedQty': str(_twap_filled_total),
                    'pricedFilled': str(_twap_priced_filled_total),
                    'twap': True,
                    'twapSlices': _twap_order_count,
                    'orderIds': list(_twap_order_ids),  # 🌟 P2: 分片订单 ID 列表
                }

            logger.info(
                f"[Binance平仓TWAP] 启动 {_symbol} {close_side} 目标={_close_qty} "
                f"分片={_twap_slices} 间隔={_twap_interval:.2f}s reduceOnly=True "
                f"mode={'EMERGENCY' if emergency else 'NORMAL'}")

            # 路径 A: binance_executor 可用 (WS 已连接)
            if self.binance_executor:
                _exec_cap = _close_qty
                for _idx in range(_twap_slices):
                    _remaining = min(state.binance_filled_qty, _exec_cap)
                    if _remaining <= _dust:
                        break
                    _slice_qty = _next_slice_qty(_remaining, _idx)
                    if _slice_qty <= _dust:
                        logger.warning(
                            f"[Binance平仓TWAP/executor] 剩余 {_remaining} 低于最小可下单量 {_bn_min_qty}，停止分片")
                        break

                    result = await self.binance_executor.place_market_order(
                        _symbol, close_side, _slice_qty, reduce_only=True,
                        position_side=_position_side or None)
                    _status = result.get('status', 'unknown') if result else 'None'
                    if result and _status in ('FILLED', 'PARTIALLY_FILLED'):
                        _filled = _record_twap_fill(result, _slice_qty, "executor")
                        _exec_cap = max(_exec_cap - _filled, Decimal('0'))
                        if _filled <= 0:
                            logger.warning(f"[Binance平仓TWAP/executor] 返回{_status}但成交量为0，停止分片")
                            break
                    else:
                        logger.error(f"[Binance平仓TWAP/executor] 下单失败 status={_status}，转REST降级")
                        break

                    if _idx < (_twap_slices - 1) and state.binance_filled_qty > _dust:
                        await asyncio.sleep(_twap_interval)

                if state.binance_filled_qty <= _dust:
                    _twap_result = _build_twap_result('FILLED')
                    if _twap_result:
                        logger.info(
                            f"✅ Binance 对冲腿已平仓(TWAP): {_symbol} "
                            f"数量={_twap_result.get('executedQty')} 均价={_twap_result.get('avgPrice')} "
                            f"分片={_twap_result.get('twapSlices')}")
                        asyncio.create_task(tg_notifier.send_async(f"✅ Binance 对冲平仓(TWAP): {_symbol}"))
                        return _twap_result
                    return {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

            # 路径 B: REST 降级 — executor 为 None 或 executor 下单未完全成交
            _bn_auth = getattr(self, 'binance_auth', None)
            if _bn_auth:
                if state.binance_filled_qty <= _dust:
                    _twap_result = _build_twap_result('FILLED')
                    return _twap_result or {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

                # 降级前再次校验交易所总仓位，防止过量 reduce_only 报错
                _actual_total_qty, _, _, _actual_known = await self._get_binance_actual_position(
                    _symbol, _position_side)
                if not _actual_known:
                    logger.warning(f"[Binance REST 降级] 无法确认交易所真实持仓({_symbol})，保留状态等待重试")
                    return None
                if _actual_total_qty <= _dust:
                    state.binance_filled_qty = Decimal('0')
                    logger.info(f"✅ Binance 交易所仓位已为0，自动清除组合残量: {_symbol}")
                    _twap_result = _build_twap_result('FILLED')
                    return _twap_result or {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

                _session = None
                _rest_cap = min(state.binance_filled_qty, _actual_total_qty)
                logger.warning(
                    f"[Binance REST 降级/TWAP] 平仓 {_symbol} {close_side} {_rest_cap} "
                    f"(分片={_twap_slices}, 间隔={_twap_interval:.2f}s)")
                try:
                    _session = aiohttp.ClientSession(
                        headers=_bn_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                    for _idx in range(_twap_slices):
                        _remaining = min(state.binance_filled_qty, _rest_cap)
                        if _remaining <= _dust:
                            break
                        _slice_qty = _next_slice_qty(_remaining, _idx)
                        if _slice_qty <= _dust:
                            logger.warning(
                                f"[Binance平仓TWAP/REST] 剩余 {_remaining} 低于最小可下单量 {_bn_min_qty}，停止分片")
                            break

                        # 🌟 P1-8: REST 降级路径补 newClientOrderId 幂等保护
                        _rest_cid = f"twap_rest_{int(time.time() * 1000)}_{_idx}"
                        _params = {
                            "symbol": _symbol,
                            "side": close_side,
                            "type": "MARKET",
                            "quantity": str(_slice_qty),
                            "newClientOrderId": _rest_cid,
                            "newOrderRespType": "RESULT"
                        }
                        if _position_side in ("LONG", "SHORT", "BOTH"):
                            _params["positionSide"] = _position_side
                        if _position_side not in ("LONG", "SHORT"):
                            _params["reduceOnly"] = "true"
                        _params = _bn_auth.sign(_params)

                        result = await self._binance_rest_fallback(
                            _bn_auth, _session, "POST", "/fapi/v1/order", _params, signed=False)
                        # 🌟 P1-8: -5022 Duplicate 幂等恢复 (查询真实订单)
                        if isinstance(result, dict) and result.get('code') in (-5022, "-5022"):
                            logger.warning(f"[Binance平仓TWAP/REST] -5022 Duplicate, 查询 cid={_rest_cid}")
                            _q_params = _bn_auth.sign({"symbol": _symbol, "origClientOrderId": _rest_cid})
                            _q_result = await self._binance_rest_fallback(
                                _bn_auth, _session, "GET", "/fapi/v1/order", _q_params, signed=False)
                            if isinstance(_q_result, dict) and _q_result.get('orderId'):
                                result = _q_result
                                logger.info(f"[Binance平仓TWAP/REST] ✅ 查单恢复: status={result.get('status')}")
                        _status = result.get('status', 'unknown') if result else 'no_response'
                        if result and _status in ('FILLED', 'PARTIALLY_FILLED'):
                            _filled = _record_twap_fill(result, _slice_qty, "REST")
                            _rest_cap = max(_rest_cap - _filled, Decimal('0'))
                            if _filled <= 0:
                                logger.warning("[Binance平仓TWAP/REST] 返回成交状态但成交量为0，停止分片")
                                break
                        else:
                            logger.error(f"[Binance平仓TWAP/REST] 下单失败 status={_status}: {result}")
                            break

                        if _idx < (_twap_slices - 1) and state.binance_filled_qty > _dust and _rest_cap > _dust:
                            await asyncio.sleep(_twap_interval)
                finally:
                    if _session:
                        await _session.close()

                if state.binance_filled_qty <= _dust:
                    _twap_result = _build_twap_result('FILLED')
                    if _twap_result:
                        logger.info(
                            f"✅ [REST降级] Binance 对冲腿已平仓(TWAP): {_symbol} "
                            f"数量={_twap_result.get('executedQty')} 均价={_twap_result.get('avgPrice')} "
                            f"分片={_twap_result.get('twapSlices')}")
                        asyncio.create_task(tg_notifier.send_async(f"✅ [REST降级] Binance 对冲平仓(TWAP): {_symbol}"))
                        return _twap_result
                    return {'status': 'FILLED', 'avgPrice': '0', 'executedQty': '0', 'reconciled': True}

                logger.warning(
                    f"⚠️ Binance 对冲腿仍有残余数量: {_symbol} 剩余={state.binance_filled_qty} "
                    f"(本轮TWAP已平={_twap_filled_total})")
                if _twap_filled_total > 0:
                    # 🌟 P2-1 修复: 部分成交本轮的 orderIds 必须在 return None 之前就进入 state
                    # 否则上层大多数调用方 "if bn_result:" 判定为 False → _capture_binance_close_ids 不被调用
                    # → Future_ID 审计链丢失本轮已成交的 orderIds
                    # 此处直接把部分成交的 orderIds 塞给 state, 与上层的 capture 互不干扰 (幂等追加)
                    self._capture_binance_close_ids(state, {
                        'orderIds': list(_twap_order_ids),
                        'status': 'PARTIALLY_FILLED',
                    })
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"⚠️ Binance 平仓部分完成(TWAP): {_symbol}\n"
                        f"本轮已平: {_twap_filled_total} 剩余: {state.binance_filled_qty}\n"
                        f"已记录订单 ID: {','.join(_twap_order_ids) or '无'}", "partial_close"))
                return None

            logger.error(f"Binance 平仓失败: 无 executor 且无 binance_auth, 无法平仓 {_symbol}")
            return None
        except Exception as e:
            logger.error(f"Binance 平仓异常: {e}")
            return None

    async def _run_settlement_twap(self, state: ArbitrageState):
        """结算窗口 TWAP 平仓：在 Deribit 30 分钟 TWAP 窗口内分片平仓 Binance 对冲腿

        目的: Deribit 期权按 07:30-08:00 UTC 的 30 分钟 TWAP 结算，而 Binance 永续只能按现价平仓。
        如果在 08:00 后才平仓，BTC 价格可能已偏移数百美元 (实测 20APR26: 漂移 $198, 损失 $19.85/0.1BTC)。
        在 TWAP 窗口内分片平仓，使 Binance 平均成交价贴近 Deribit 结算 TWAP，消除时序错配风险。

        结果存储在 state._settlement_twap_result，供 _handle_delivery_settlement 直接使用。
        """
        expiry, strike = state.expiry_strike
        log_prefix = f"[{expiry}-{int(strike)}]"
        _dust = Decimal('0.0001')

        _acc_filled = Decimal('0')
        _acc_notional = Decimal('0')
        _acc_priced_filled = Decimal('0')
        _acc_last_avg = Decimal('0')
        _acc_orders: List[str] = []
        _acc_slices = 0
        _filled_total = Decimal('0')
        _notional_total = Decimal('0')
        _priced_filled_total = Decimal('0')
        _order_count = 0
        _order_ids: List[str] = []
        _last_avg = Decimal('0')

        try:
            _acc = getattr(state, '_settlement_twap_accumulated', {})
            if not isinstance(_acc, dict):
                _acc = {}
            try:
                _acc_filled = Decimal(str(_acc.get('filled', 0)))
                _acc_notional = Decimal(str(_acc.get('notional', 0)))
                _acc_priced_filled = Decimal(str(_acc.get('pricedFilled', 0)))
                _acc_last_avg = Decimal(str(_acc.get('lastAvg', 0)))
                _acc_orders = list(_acc.get('orderIds', []))
                _acc_slices = int(_acc.get('slices', 0))
                if not _acc_filled.is_finite() or _acc_filled < 0:
                    _acc_filled = Decimal('0')
                if not _acc_notional.is_finite() or _acc_notional < 0:
                    _acc_notional = Decimal('0')
                if not _acc_priced_filled.is_finite() or _acc_priced_filled < 0:
                    _acc_priced_filled = Decimal('0')
                if not _acc_last_avg.is_finite() or _acc_last_avg < 0:
                    _acc_last_avg = Decimal('0')
                if _acc_priced_filled <= 0 and _acc_filled > 0 and _acc_notional > 0:
                    if _acc_last_avg > 0:
                        _acc_priced_filled = min(_acc_filled, _acc_notional / _acc_last_avg)
                    else:
                        _acc_priced_filled = _acc_filled
                if _last_avg <= 0 and _acc_last_avg > 0:
                    _last_avg = _acc_last_avg
            except (ValueError, TypeError, ArithmeticError) as _acc_err:
                logger.warning(f"{log_prefix} 结算TWAP: 累积数据解析异常 {_acc_err}，从零开始")
                _acc_filled = Decimal('0')
                _acc_notional = Decimal('0')
                _acc_priced_filled = Decimal('0')
                _acc_last_avg = Decimal('0')
                _acc_orders = []
                _acc_slices = 0

            if not state.binance_future_symbol or state.binance_filled_qty <= _dust:
                return

            _symbol = state.binance_future_symbol
            _position_side = (state.binance_position_side or "").upper()
            if _position_side not in ("LONG", "SHORT") and self.binance_dual_side_mode:
                if state.strategy_type == 'buy_future_sell_synthetic':
                    _position_side = "LONG"
                elif state.strategy_type == 'sell_future_buy_synthetic':
                    _position_side = "SHORT"

            if state.strategy_type == 'buy_future_sell_synthetic':
                close_side = 'SELL'
            elif state.strategy_type == 'sell_future_buy_synthetic':
                close_side = 'BUY'
            else:
                logger.error(f"{log_prefix} 结算TWAP: 无法确定平仓方向, strategy={state.strategy_type}")
                return

            _total_qty = state.binance_filled_qty
            try:
                _actual_qty, _, _, _actual_known = await self._get_binance_actual_position(
                    _symbol, _position_side)
                if _actual_known and _actual_qty <= _dust:
                    logger.warning(f"{log_prefix} 结算TWAP: 交易所实际仓位为0，跳过TWAP")
                    return
                if _actual_known and _actual_qty < _total_qty:
                    logger.warning(
                        f"{log_prefix} 结算TWAP: 本地={_total_qty} > 交易所总仓={_actual_qty}，"
                        f"夹紧到交易所总仓（不修改 state，仅限本次 TWAP）")
                    _total_qty = _actual_qty
            except Exception as e:
                logger.warning(f"{log_prefix} 结算TWAP: 仓位验证异常 {e}，以本地记录为准")
            state._settlement_twap_qty_snapshot = _total_qty
            _slices = max(int(getattr(self, 'settlement_twap_slices', 30)), 2)
            _twap_minutes = max(float(getattr(self, 'settlement_twap_minutes', 30)), 5)
            _interval_sec = (_twap_minutes * 60) / _slices

            _bn_info = (getattr(self.binance_executor, 'contract_info', {}).get(_symbol, {})
                        if self.binance_executor else {})
            _bn_step = Decimal(str(_bn_info.get('step_size', '0.001') or '0.001'))
            if _bn_step <= 0:
                _bn_step = Decimal('0.001')
            _bn_min_qty = Decimal(str(_bn_info.get('min_qty', _bn_step) or _bn_step))
            if _bn_min_qty <= 0:
                _bn_min_qty = _bn_step

            def _round_qty(qty: Decimal) -> Decimal:
                if qty <= 0:
                    return Decimal('0')
                if self.binance_executor:
                    try:
                        return self.binance_executor._round_qty(_symbol, qty)
                    except Exception:
                        pass
                return (qty // _bn_step) * _bn_step if _bn_step > 0 else qty

            logger.info(
                f"{log_prefix} 🕐 结算TWAP启动: {_symbol} {close_side} 总量={_total_qty} "
                f"分片={_slices} 间隔={_interval_sec:.0f}s 窗口={_twap_minutes:.0f}min")

            _consecutive_failures = 0
            _max_consecutive_failures = 3

            _close_remaining = _total_qty

            for i in range(_slices):
                if state.state not in ('position_open', 'executing'):
                    logger.info(f"{log_prefix} 结算TWAP: 状态变更为 {state.state}，停止")
                    break

                _remaining = min(state.binance_filled_qty, _close_remaining)
                if _remaining <= _dust:
                    logger.info(f"{log_prefix} 结算TWAP: Binance 仓位已清零，提前完成")
                    break

                _left_slices = max(_slices - i, 1)
                _target_qty = _remaining if _left_slices <= 1 else (_remaining / Decimal(str(_left_slices)))
                _slice_qty = _round_qty(_target_qty)
                if _slice_qty < _bn_min_qty:
                    _slice_qty = _round_qty(_remaining)
                if _slice_qty < _bn_min_qty:
                    logger.warning(f"{log_prefix} 结算TWAP: 剩余 {_remaining} 低于最小量 {_bn_min_qty}，停止")
                    break
                if _slice_qty > _remaining:
                    _slice_qty = _round_qty(_remaining)

                try:
                    if not self.binance_executor:
                        logger.warning(f"{log_prefix} 结算TWAP [{i+1}/{_slices}]: executor 不可用，等待下一片")
                        if i < _slices - 1:
                            await asyncio.sleep(_interval_sec)
                        continue

                    result = await self.binance_executor.place_market_order(
                        _symbol, close_side, _slice_qty, reduce_only=True,
                        position_side=_position_side or None)

                    _status = result.get('status', 'unknown') if result else 'None'
                    if result and _status in ('FILLED', 'PARTIALLY_FILLED'):
                        _filled = Decimal(str(result.get('executedQty', '0'))) if result else Decimal('0')
                        if _filled <= 0 and _status == 'FILLED':
                            _filled = _slice_qty
                        _raw_avg = _decimal_or_zero(result.get('avgPrice')) if isinstance(result, dict) else Decimal('0')
                        _avg = _extract_binance_fill_avg(result, _filled, _last_avg if _last_avg > 0 else _acc_last_avg)

                        state.binance_filled_qty = max(state.binance_filled_qty - _filled, Decimal('0'))
                        if state.binance_filled_qty <= _dust:
                            state.binance_filled_qty = Decimal('0')
                        _close_remaining = max(_close_remaining - _filled, Decimal('0'))

                        _filled_total += _filled
                        _order_count += 1
                        _consecutive_failures = 0
                        if _avg > 0:
                            if _raw_avg <= 0:
                                logger.warning(
                                    f"{log_prefix} 结算TWAP [{i+1}/{_slices}]: "
                                    f"Binance avgPrice=0，已用成交额或上一片均价恢复为 {_avg}")
                            _last_avg = _avg
                            _notional_total += _avg * _filled
                            _priced_filled_total += _filled
                        elif _filled > 0:
                            logger.warning(
                                f"{log_prefix} 结算TWAP [{i+1}/{_slices}]: "
                                f"成交={_filled} 但均价缺失，本片不计入 VWAP 定价分母")

                        _oid = result.get('orderId')
                        if _oid:
                            _order_ids.append(str(_oid))

                        logger.info(
                            f"{log_prefix} 结算TWAP [{i+1}/{_slices}]: "
                            f"成交={_filled} 均价={_avg} 剩余={state.binance_filled_qty}")

                        state._settlement_twap_accumulated = {
                            'filled': float(_filled_total + _acc_filled),
                            'notional': float(_notional_total + _acc_notional),
                            'pricedFilled': float(_priced_filled_total + _acc_priced_filled),
                            'orderIds': _acc_orders + _order_ids,
                            'slices': _acc_slices + _order_count,
                            'lastAvg': float(_last_avg),
                        }
                        # 🌟 2026-04-24 新增: 记录最近一次 Binance 对冲分片成功关闭时间
                        # 用于 '实际对冲关闭时间' 字段，让年化收益率不受 delivery 写入时差影响
                        state._hedge_close_completed_ts = time.time()
                        try:
                            await self._save_state_to_redis(state)
                        except Exception as _redis_err:
                            logger.warning(f"{log_prefix} 结算TWAP分片持久化失败: {_redis_err}")
                    else:
                        _consecutive_failures += 1
                        logger.warning(
                            f"{log_prefix} 结算TWAP [{i+1}/{_slices}]: "
                            f"下单失败 status={_status}，连续失败={_consecutive_failures}")
                        if _consecutive_failures >= _max_consecutive_failures:
                            logger.error(f"{log_prefix} 结算TWAP: 连续{_consecutive_failures}次失败，中止TWAP")
                            break
                except Exception as e:
                    _consecutive_failures += 1
                    logger.error(f"{log_prefix} 结算TWAP [{i+1}/{_slices}] 异常: {e}, 连续失败={_consecutive_failures}")
                    if _consecutive_failures >= _max_consecutive_failures:
                        logger.error(f"{log_prefix} 结算TWAP: 连续{_consecutive_failures}次异常，中止TWAP")
                        break

                if i < _slices - 1 and state.binance_filled_qty > _dust:
                    await asyncio.sleep(_interval_sec)

            _grand_filled = _filled_total + _acc_filled
            _grand_notional = _notional_total + _acc_notional
            _grand_priced_filled = _priced_filled_total + _acc_priced_filled
            _grand_orders = _acc_orders + _order_ids
            _grand_slices = _acc_slices + _order_count

            if _grand_filled > 0:
                _grand_avg = _calculate_twap_vwap(
                    _grand_notional, _grand_filled, _grand_priced_filled,
                    _last_avg if _last_avg > 0 else _acc_last_avg)
                state._settlement_twap_result = {
                    'status': 'FILLED' if state.binance_filled_qty <= _dust else 'PARTIALLY_FILLED',
                    'avgPrice': str(_grand_avg),
                    'executedQty': str(_grand_filled),
                    'pricedFilled': str(_grand_priced_filled),
                    'twap': True,
                    'twapSlices': _grand_slices,
                    'orderIds': _grand_orders,
                    'settlement_twap': True,
                }
                logger.info(
                    f"{log_prefix} 结算TWAP完成: 总成交={_grand_filled} VWAP={_grand_avg:.2f} "
                    f"分片={_grand_slices}(本轮{_order_count}) 剩余={state.binance_filled_qty}")
            else:
                if _acc_filled > 0:
                    _grand_avg = _calculate_twap_vwap(
                        _acc_notional, _acc_filled, _acc_priced_filled,
                        _last_avg if _last_avg > 0 else _acc_last_avg)
                    state._settlement_twap_result = {
                        'status': 'PARTIALLY_FILLED',
                        'avgPrice': str(_grand_avg),
                        'executedQty': str(_acc_filled),
                        'pricedFilled': str(_acc_priced_filled),
                        'twap': True,
                        'twapSlices': _acc_slices,
                        'orderIds': _acc_orders,
                        'settlement_twap': True,
                    }
                logger.warning(f"{log_prefix} 结算TWAP: 本轮全部分片均未成交"
                               f"{'（含前轮累计 ' + str(_acc_filled) + '）' if _acc_filled > 0 else ''}")

        except asyncio.CancelledError:
            _c_grand_filled = _filled_total + _acc_filled
            _c_grand_notional = _notional_total + _acc_notional
            _c_grand_priced_filled = _priced_filled_total + _acc_priced_filled
            if _c_grand_filled > 0:
                _c_avg = _calculate_twap_vwap(
                    _c_grand_notional, _c_grand_filled, _c_grand_priced_filled,
                    _last_avg if _last_avg > 0 else _acc_last_avg)
                state._settlement_twap_result = {
                    'status': 'PARTIALLY_FILLED',
                    'avgPrice': str(_c_avg),
                    'executedQty': str(_c_grand_filled),
                    'pricedFilled': str(_c_grand_priced_filled),
                    'twap': True,
                    'twapSlices': _acc_slices + _order_count,
                    'orderIds': _acc_orders + _order_ids,
                    'settlement_twap': True,
                    'cancelled': True,
                }
                try:
                    await self._save_state_to_redis(state)
                except Exception:
                    pass
            logger.info(f"{log_prefix} 结算TWAP任务被取消 (本轮={_filled_total}, 累计={_c_grand_filled})")
            raise
        except Exception as e:
            _e_grand_filled = _filled_total + _acc_filled
            _e_grand_notional = _notional_total + _acc_notional
            _e_grand_priced_filled = _priced_filled_total + _acc_priced_filled
            if _e_grand_filled > 0:
                _e_avg = _calculate_twap_vwap(
                    _e_grand_notional, _e_grand_filled, _e_grand_priced_filled,
                    _last_avg if _last_avg > 0 else _acc_last_avg)
                state._settlement_twap_result = {
                    'status': 'PARTIALLY_FILLED',
                    'avgPrice': str(_e_avg),
                    'executedQty': str(_e_grand_filled),
                    'pricedFilled': str(_e_grand_priced_filled),
                    'twap': True,
                    'twapSlices': _acc_slices + _order_count,
                    'orderIds': _acc_orders + _order_ids,
                    'settlement_twap': True,
                    'error': str(e),
                }
            logger.error(f"{log_prefix} 结算TWAP异常: {e}")

    async def _recover_stuck_binance_after_settlement(self, state: ArbitrageState):
        """🌟 C 修复: 交割记录已写但 Binance 残余未清时的恢复分支

        场景: Deribit 期权已结算且记录已落盘 (state._delivery_csv_written=True),
              但 Binance 关单失败, 组合登记在 _binance_close_failed_combos。
              Monitor 下一秒再次检测到 Deribit 期权位置=0 会再次进入 _handle_delivery_settlement,
              若不做路由保护, 会重复 P&L 计算 + 重复落盘 → 审计污染。
        做法: 只调 _close_binance_hedge, 成功后清理 state, 不再做计算/记录。
        """
        _key = state.expiry_strike
        log_prefix = f"[{_key[0]}-{_key[1]}]"
        if state.binance_filled_qty <= Decimal('0.0001'):
            # 残余已清, 结束此组合
            logger.info(f"{log_prefix} Binance 残余已清 (qty={state.binance_filled_qty}), 释放组合")
            self._binance_close_failed_combos.discard(_key)
            self._remove_pause("Binance残余仓位")
            state.state = 'exited'
            state.last_update = time.time()
            await self._delete_state_from_redis(_key[0], _key[1])
            self.position_locks.discard(_key)
            self._bn_mark_missing_since.pop(_key, None)
            self._bn_mark_degraded_log_ts.pop(_key, None)
            self._broken_combo_first_seen.pop(_key, None)
            self._broken_combo_handling.discard(_key)
            self._combo_closing_locks.pop(_key, None)
            return
        logger.warning(f"{log_prefix} 🚑 进入 Binance 残余恢复分支 (qty={state.binance_filled_qty}), 重试关单")
        _bn_result = await self._close_binance_hedge(state, emergency=True)
        if _bn_result and state.binance_filled_qty <= Decimal('0.0001'):
            # 🌟 P2 回归修复: 补关成功的 orderIds 也要保存, 保持审计链完整
            self._capture_binance_close_ids(state, _bn_result)
            logger.info(f"{log_prefix} ✅ Binance 残余清理成功")
            self._binance_close_failed_combos.discard(_key)
            self._remove_pause("Binance残余仓位")
            state.state = 'exited'
            state.last_update = time.time()
            await self._delete_state_from_redis(_key[0], _key[1])
            self.position_locks.discard(_key)
            self._bn_mark_missing_since.pop(_key, None)
            self._bn_mark_degraded_log_ts.pop(_key, None)
            self._broken_combo_first_seen.pop(_key, None)
            self._broken_combo_handling.discard(_key)
            self._combo_closing_locks.pop(_key, None)
            asyncio.create_task(tg_notifier.send_async(
                f"✅ {_key[0]}-{_key[1]} Binance 残余已清理, 组合关闭"))
        else:
            logger.warning(f"{log_prefix} Binance 残余仍未清 (qty={state.binance_filled_qty}), 将下轮重试")
            state.last_update = time.time()
            self._add_pause("Binance残余仓位")
            # 🌟 P2 回归修复 (对称于 _handle_delivery_settlement_locked 未完成分支):
            #   本轮 _close_binance_hedge 内部 P2-1 已 capture 部分成交的 orderIds 到
            #   state.binance_close_order_id (内存), _apply_combo_fill 也扣减了
            #   state.binance_filled_qty (内存). 不 save 到 Redis 的话, 进程 crash 后
            #   重启会基于过期快照重复处理 + 审计链丢本轮 orderIds.
            try:
                await self._save_state_to_redis(state)
            except Exception as _save_err:
                logger.error(f"{log_prefix} 残余恢复失败路径持久化失败: {_save_err}")

    async def _handle_delivery_settlement(self, state: ArbitrageState, combination: dict):
        """处理 Deribit 期权到期交割：关闭 Binance 对冲、计算盈亏、记录落盘、发送 Telegram

        交割流程：
        1. 获取当前 BTC 价格作为结算价近似值
        2. 计算期权到期结算价值 (call/put payoff)
        3. 关闭 Binance 对冲腿 (部分平仓)
        4. 计算往返盈亏 = Deribit 期权 P&L + Binance 期货 P&L - 手续费
        5. 写入交易记录 / 发送 Telegram / 清理状态
        """
        # 🌟 B 修复: 以 per-combo 锁包裹, 与 _emergency_dump_all 串行
        _closing_lock = self._get_closing_lock(state)
        async with _closing_lock:
            return await self._handle_delivery_settlement_locked(state, combination)

    async def _handle_delivery_settlement_locked(self, state: ArbitrageState, combination: dict):
        """原 _handle_delivery_settlement 主体, 通过锁包裹后的内部实现"""
        # 🌟 B 修复: 进入临界区后二次校验状态 — 若已完全结算/清理则跳过
        # 🌟 C 修复: 若记录已落盘说明上次结算已完成, 二次进入会造成重复记录, 强制跳过
        if self._is_deribit_settlement_core_window():
            _now = time.time()
            if _now - getattr(state, '_delivery_core_guard_log_ts', 0.0) >= 30:
                state._delivery_core_guard_log_ts = _now
                logger.warning(
                    f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] ⏸️ "
                    f"Deribit core settlement window active，交割处理延后")
            return
        if state.state in ('exited', 'closed', 'cleaned'):
            logger.info(f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] 交割入口: "
                        f"仓位已被其他路径处理 (state={state.state}), 跳过")
            return
        if getattr(state, '_delivery_csv_written', False):
            logger.warning(f"[{state.expiry_strike[0]}-{state.expiry_strike[1]}] 交割入口: "
                           f"记录已落盘 (上次结算后 Binance 关单异常重入), 仅恢复残余 Binance 对冲")
            # 进入恢复分支: 只尝试关 Binance 对冲, 不再算 P&L / 写记录
            await self._recover_stuck_binance_after_settlement(state)
            return
        expiry, strike = state.expiry_strike
        log_prefix = f"[{expiry}-{int(strike)}]"
        entry_amount = state.entry_amount or Decimal('0.1')

        # ===== 1. 获取结算价格 (多级降级) =====
        settlement_price = Decimal('0')
        # 优先获取 Deribit 官方结算价 (30分钟TWAP)
        try:
            _delivery_resp = await self.client.send_request({
                "jsonrpc": "2.0",
                "id": self.client._get_next_request_id(),
                "method": "public/get_delivery_prices",
                "params": {"index_name": f"{self.target_currency.lower()}_usd", "count": 1}
            })
            if _delivery_resp and 'result' in _delivery_resp:
                _del_data = _delivery_resp['result'].get('data', [])
                if _del_data:
                    settlement_price = Decimal(str(_del_data[0].get('delivery_price', 0)))
                    if settlement_price > 0:
                        logger.info(f"{log_prefix} 使用 Deribit 官方结算价: {settlement_price}")
        except Exception as e:
            logger.info(f"{log_prefix} 获取官方结算价失败: {e}")
        # 降级方案: 参考现货价(Binance盘口/标记/最新价/指数价) -> Deribit 指数价 -> Deribit 永续中间价
        if settlement_price <= 0:
            try:
                _settle_perp = state.binance_future_symbol or (
                    self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
                )
                _ref_px = await self._get_reference_btc_price(_settle_perp)
                if _ref_px > 0:
                    settlement_price = _ref_px
                    logger.warning(f"{log_prefix} 官方结算价不可用，降级使用参考价: {settlement_price}")
            except Exception:
                pass
        if settlement_price <= 0:
            try:
                _idx_resp = await self.client.send_request({
                    "jsonrpc": "2.0", "id": self.client._get_next_request_id(),
                    "method": "public/get_index_price",
                    "params": {"index_name": f"{self.target_currency.lower()}_usd"}
                })
                if 'result' in _idx_resp:
                    settlement_price = Decimal(str(_idx_resp['result'].get('index_price', 0)))
            except Exception:
                pass
        if settlement_price <= 0:
            try:
                _dr_perp = f"{self.target_currency}-PERPETUAL"
                _dr_t = self.client.tickers.get(_dr_perp)
                if _dr_t and _dr_t.mid_price > 0:
                    settlement_price = _dr_t.mid_price
            except Exception:
                pass
        if settlement_price <= 0:
            # 重试计数器：超过5次强制用 Binance 盘口价 / 否则延迟重试
            _settle_retries = getattr(state, '_settle_retries', 0) + 1
            state._settle_retries = _settle_retries
            # 超过 5 次后每 60 秒才重试一次（避免每秒都查询 Binance API）
            if _settle_retries > 5:
                _settle_last_attempt = getattr(state, '_settle_last_attempt_ts', 0.0)
                _settle_elapsed = time.time() - _settle_last_attempt
                if _settle_elapsed < 60:
                    logger.info(f"{log_prefix} 结算价重试节流中，约{60 - int(_settle_elapsed)}秒后重试")
                    await self._save_state_to_redis(state)
                    return
                state._settle_last_attempt_ts = time.time()
            if _settle_retries > 5:
                logger.error(f"{log_prefix} 结算价获取连续失败 {_settle_retries} 次，尝试 Binance 盘口价兜底")
                _settle_perp = state.binance_future_symbol or (self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT')
                _bn_ob = self.binance_ws.order_books.get(_settle_perp) if self.binance_ws else None
                _bn_ob_age = time.time() - _bn_ob.update_time if _bn_ob and getattr(_bn_ob, 'update_time', 0) else float('inf')
                if _bn_ob and _bn_ob.mid_price is not None and _bn_ob.mid_price > 0 and _bn_ob_age <= 30:
                    settlement_price = _bn_ob.mid_price
                elif _bn_ob and _bn_ob.mid_price is not None and _bn_ob.mid_price > 0:
                    settlement_price = Decimal('0')
                    logger.warning(f"{log_prefix} Binance 结算价兜底盘口过期 {_bn_ob_age:.1f}s，拒绝使用")
                else:
                    settlement_price = Decimal('0')
                if settlement_price <= 0:
                    logger.error(
                        f"{log_prefix} 🚨 结算价完全不可用 (Deribit 多级降级失败 + Binance 盘口也无数据)\n"
                        f"等待人工介入. 状态保持 position_open, {60}秒后重试。")
                    asyncio.create_task(tg_notifier.send_error_async(
                        f"🚨 {log_prefix} 交割结算价无法获取!\n"
                        f"Deribit 官方/指数/永续 全部失败, Binance 盘口也无数据\n"
                        f"已重试 {_settle_retries} 次, 每60秒重试一次\n"
                        f"⚠️ 不会用错误价格强行结算以免污染 P&L 记录",
                        "settlement_price_unavailable"))
                    state.last_update = time.time()
                    await self._save_state_to_redis(state)
                    return
                # 不 return，继续执行关闭流程 (Binance 盘口价虽不完美但可用)
                logger.warning(f"{log_prefix} 使用 Binance 盘口价 {settlement_price} 作结算价 (非 Deribit 官方 TWAP)")
            else:
                logger.error(f"{log_prefix} 交割结算: 无法获取 {self.target_currency} 价格，延迟重试 ({_settle_retries}/5)")
                state.last_update = time.time()
                await self._save_state_to_redis(state)
                return

        # ===== 2. 计算期权到期结算价值 =====
        strike_dec = Decimal(str(strike))
        call_settlement = max(Decimal('0'), (settlement_price - strike_dec) / settlement_price)
        put_settlement = max(Decimal('0'), (strike_dec - settlement_price) / settlement_price)

        # ===== 3. 关闭 Binance 对冲腿 (部分平仓) =====
        # 结算TWAP 预平仓处理: 若 _run_settlement_twap 已在 TWAP 窗口内提前平仓，直接使用其结果
        _twap_result = getattr(state, '_settlement_twap_result', None)
        _twap_task = getattr(state, '_settlement_twap_task', None)
        _twap_qty_snapshot = getattr(state, '_settlement_twap_qty_snapshot', Decimal('0'))

        # 崩溃恢复兜底: _twap_result 不持久化，但 _accumulated 已持久化到 Redis
        # 若 result 为空但 accumulated 有成交记录，从 accumulated 重建等效 result
        if not _twap_result:
            _twap_acc = getattr(state, '_settlement_twap_accumulated', None)
            _acc_filled = _decimal_or_zero(_twap_acc.get('filled')) if isinstance(_twap_acc, dict) else Decimal('0')
            if isinstance(_twap_acc, dict) and _acc_filled > 0:
                _acc_notional = _decimal_or_zero(_twap_acc.get('notional'))
                _acc_priced_filled = _decimal_or_zero(_twap_acc.get('pricedFilled'))
                _acc_last_avg = _decimal_or_zero(_twap_acc.get('lastAvg'))
                if _acc_priced_filled <= 0 and _acc_notional > 0:
                    if _acc_last_avg > 0:
                        _acc_priced_filled = min(_acc_filled, _acc_notional / _acc_last_avg)
                    else:
                        _acc_priced_filled = _acc_filled
                _acc_avg = _calculate_twap_vwap(
                    _acc_notional, _acc_filled, _acc_priced_filled, _acc_last_avg)
                _twap_result = {
                    'status': 'PARTIALLY_FILLED',
                    'avgPrice': str(_acc_avg),
                    'executedQty': str(_acc_filled),
                    'pricedFilled': str(_acc_priced_filled),
                    'twap': True,
                    'twapSlices': int(_twap_acc.get('slices', 0)),
                    'orderIds': list(_twap_acc.get('orderIds', [])),
                    'settlement_twap': True,
                    'reconstructed_from_accumulated': True,
                }
                state._settlement_twap_result = _twap_result
                logger.info(
                    f"{log_prefix} 交割结算: 从持久化累积数据重建TWAP结果 "
                    f"filled={_acc_filled} avg={_twap_result['avgPrice']}")

        # 若 TWAP 任务仍在运行，取消并等待其真正终止后再继续
        if _twap_task and not _twap_task.done():
            logger.info(f"{log_prefix} 交割结算: 取消进行中的结算TWAP任务")
            _twap_task.cancel()
            try:
                await _twap_task
            except (asyncio.CancelledError, Exception):
                pass
            _twap_result = getattr(state, '_settlement_twap_result', None)

        bn_qty = state.binance_filled_qty
        # 若 TWAP 已部分/全部清仓，用 TWAP 快照还原原始数量 (费用计算需要)
        if bn_qty <= Decimal('0.0001') and _twap_result and _twap_qty_snapshot > 0:
            bn_qty = _twap_qty_snapshot
        bn_entry = state.binance_entry_price or state.entry_prices.get('future', Decimal('0'))
        bn_open_qty = state.binance_open_qty if getattr(state, 'binance_open_qty', Decimal('0')) > 0 else bn_qty
        # TWAP 快照修正: 部分成交时 bn_qty 只是剩余量, bn_open_qty 需用原始开仓量
        if bn_open_qty < _twap_qty_snapshot and _twap_qty_snapshot > 0:
            bn_open_qty = _twap_qty_snapshot
        if bn_open_qty <= 0 and bn_entry > 0 and state.future_size_usd > 0:
            bn_open_qty = (state.future_size_usd / bn_entry)
        bn_close_price = Decimal('0')

        if state.binance_future_symbol:
            _bn_dust = Decimal('0.0001')

            if state.binance_filled_qty <= _bn_dust and _twap_result:
                # 路径 A: 结算TWAP 已完全关闭 Binance 仓位
                bn_result = _twap_result
                self._capture_binance_close_ids(state, bn_result)
                try:
                    _avg = bn_result.get('avgPrice', '0')
                    bn_close_price = Decimal(str(_avg)) if _avg else Decimal('0')
                    _exec_qty = Decimal(str(bn_result.get('executedQty', '0')))
                    if _exec_qty > 0:
                        bn_qty = _exec_qty
                except Exception:
                    bn_close_price = Decimal('0')
                self._binance_close_failed_combos.discard(state.expiry_strike)
                logger.info(
                    f"{log_prefix} 交割结算: Binance 已由结算TWAP提前平仓, "
                    f"VWAP={bn_close_price}, 数量={bn_qty}, 分片={bn_result.get('twapSlices', 0)}")

            elif bn_qty > _bn_dust:
                # 路径 B: TWAP 未启动/部分成交/未完成 → 走原有 _close_binance_hedge 关闭剩余
                if _twap_result:
                    logger.info(
                        f"{log_prefix} 交割结算: 结算TWAP部分成交, 剩余={state.binance_filled_qty}, 继续关闭")
                _pre_twap_filled = Decimal(str(_twap_result.get('executedQty', '0'))) if _twap_result else Decimal('0')
                _pre_twap_avg = Decimal(str(_twap_result.get('avgPrice', '0'))) if _twap_result else Decimal('0')

                bn_result = await self._close_binance_hedge(state)
                if not bn_result or state.binance_filled_qty > _bn_dust:
                    logger.warning(
                        f"{log_prefix} 交割结算: Binance 对冲平仓未完成，"
                        f"剩余={state.binance_filled_qty}，将在下次循环重试")
                    self._binance_close_failed_combos.add(state.expiry_strike)
                    self._add_pause("Binance残余仓位")
                    state.last_update = time.time()
                    try:
                        await self._save_state_to_redis(state)
                    except Exception as _save_err:
                        logger.error(f"{log_prefix} 交割未完分支持久化失败: {_save_err}")
                    return
                self._binance_close_failed_combos.discard(state.expiry_strike)
                self._capture_binance_close_ids(state, bn_result)
                # 🌟 2026-04-24: 路径B 同步关闭时间即对冲关闭时间
                state._hedge_close_completed_ts = time.time()
                try:
                    _close_avg = Decimal(str(bn_result.get('avgPrice', '0') or '0'))
                    _close_filled = Decimal(str(bn_result.get('executedQty', '0') or '0'))

                    # 合并 TWAP 部分成交 + _close_binance_hedge 成交 → 计算综合 VWAP
                    if _pre_twap_filled > 0 and _pre_twap_avg > 0 and _close_filled > 0 and _close_avg > 0:
                        _total_filled = _pre_twap_filled + _close_filled
                        bn_close_price = (_pre_twap_avg * _pre_twap_filled + _close_avg * _close_filled) / _total_filled
                        bn_qty = _total_filled
                        logger.info(
                            f"{log_prefix} 综合VWAP: TWAP成交={_pre_twap_filled}@{_pre_twap_avg:.2f} + "
                            f"即时平仓={_close_filled}@{_close_avg:.2f} → VWAP={bn_close_price:.2f}")
                    elif _close_avg > 0:
                        bn_close_price = _close_avg
                        if _close_filled > 0:
                            bn_qty = _close_filled
                    elif _pre_twap_avg > 0:
                        bn_close_price = _pre_twap_avg
                        if _pre_twap_filled > 0:
                            bn_qty = _pre_twap_filled
                except Exception:
                    bn_close_price = Decimal('0')

        # ===== 4. 计算往返盈亏 =====
        f_entry = state.entry_prices.get('future', Decimal('0'))
        c_entry = state.entry_prices.get('call', Decimal('0'))
        p_entry = state.entry_prices.get('put', Decimal('0'))
        future_size_usd = state.future_size_usd or Decimal('0')
        net_pnl_usd = 0.0
        open_fee_usd = 0.0
        close_fee_usd = 0.0
        _delivery_fee_usd = 0.0  # 默认值，防止 try 内异常时日志/记录引用未赋值
        funding_net_usd = 0.0
        _fee_source = "estimated"
        _funding_source = "none"

        try:
            # Deribit 期权盈亏 (BTC)
            if state.strategy_type == 'sell_future_buy_synthetic':
                o_pnl = (call_settlement - c_entry + p_entry - put_settlement) * entry_amount
            else:
                o_pnl = (c_entry - call_settlement + put_settlement - p_entry) * entry_amount
            deribit_pnl_usd = float(o_pnl * settlement_price)

            # Binance 期货盈亏 (USDT)
            bn_pnl_usdt = Decimal('0')
            if bn_qty > 0 and bn_close_price > 0 and bn_entry > 0:
                if state.strategy_type == 'buy_future_sell_synthetic':
                    bn_pnl_usdt = (bn_close_price - bn_entry) * bn_qty
                else:
                    bn_pnl_usdt = (bn_entry - bn_close_price) * bn_qty

            # 🌟 Plan Bug #4 修复: Binance 手续费改为本地精确计算 (不再调用 income API)
            # 原因: _get_binance_realized_commission_usd 按 symbol+时间窗查询, 同 symbol 多组合
            #       并行时会把其他组合的佣金一起算进来 (2026-04-11 11APR26 交割发现问题).
            # 本地计算公式: bn_entry × bn_qty × taker_rate (与 Binance 官方完全一致, 无精度损失).
            # taker_rate 从 self.binance_fee_calc 读取 (默认 standard tier 0.04%).
            fc = self.trade_executor.fee_calculator
            deribit_open_fee_usd = 0.0
            bn_open_fee_usd = 0.0
            bn_close_fee_usd = 0.0
            if f_entry > 0 and c_entry > 0 and p_entry > 0:
                _oc = fc.calculate_option_fee(f_entry, c_entry, entry_amount, is_taker=True)
                _op = fc.calculate_option_fee(f_entry, p_entry, entry_amount, is_taker=True)
                deribit_open_fee_usd = float((_oc + _op) * f_entry)
            # Binance taker 费率: 优先读 BinanceFeeCalculator 实例, 兜底 0.04% (standard tier)
            # 🌟 修正: RealTimeArbitrageEngine 的属性是 self.binance_fee_calc (不带下划线前缀),
            #   TradeExecutor 里是 self._binance_fee_calc (带前缀), 别混了
            _bn_taker_rate = (getattr(self.binance_fee_calc, 'taker_rate', Decimal('0.0004'))
                              if getattr(self, 'binance_fee_calc', None) is not None
                              else Decimal('0.0004'))
            if bn_entry > 0 and bn_open_qty > 0:
                bn_open_fee_usd = float(bn_entry * bn_open_qty * _bn_taker_rate)
            if bn_close_price > 0 and bn_qty > 0:
                bn_close_fee_usd = float(bn_close_price * bn_qty * _bn_taker_rate)
            open_fee_usd = deribit_open_fee_usd + bn_open_fee_usd
            close_fee_usd = bn_close_fee_usd
            _fee_source = "local_calc"  # 🌟 Plan Bug #4: 标记为本地精确计算

            # Deribit 期权交割费 (0.015% × amount × 2腿)
            # 交割费上限应使用交割时的实际结算价（intrinsic），而不是开仓权利金
            _del_c_fee = fc.calculate_delivery_fee(settlement_price, call_settlement, entry_amount, is_option=True)
            _del_p_fee = fc.calculate_delivery_fee(settlement_price, put_settlement, entry_amount, is_option=True)
            _delivery_fee_btc = _del_c_fee + _del_p_fee
            _delivery_fee_usd = float(_delivery_fee_btc * settlement_price)

            # Binance 永续 funding 净损益（从开仓时间到现在，按实际持仓时长+方向）
            # 优先读取 Binance 已实现 funding income；失败时再回退估算
            # 口径约定说明：
            # - 扫描阶段 funding_deduction_usd 采用"成本口径"(成本=正, 收入=负)
            # - 结算阶段 funding_net_usd 采用"净收益口径"(收入=正, 支出=负)
            # 两者在各自公式中均保持自洽，避免净利方向错误。
            funding_net_usd = 0.0
            _funding_source = "none"
            _funding_qty_ref = bn_open_qty if bn_open_qty > 0 else (
                bn_qty if bn_qty > 0 else (entry_amount if entry_amount > 0 else state.entry_amount))
            _has_bn_hedge_history = bool(
                state.binance_future_symbol and (
                    state.binance_entry_price > 0 or
                    bn_entry > 0 or
                    bn_open_qty > 0 or
                    _funding_qty_ref > 0 or
                    bool(state.binance_order_id)
                )
            )
            _entry_ts = state.start_time if state.start_time > 0 else 0
            if _entry_ts <= 0 and state.combo_id:
                try:
                    _entry_ts = float(state.combo_id.rsplit('-', 1)[-1])
                except Exception:
                    _entry_ts = 0
            if _entry_ts <= 0:
                _entry_ts = time.time()
            _start_ms = int(_entry_ts * 1000)
            _end_ms = int(time.time() * 1000)
            if _has_bn_hedge_history:
                try:
                    # 🌟 Plan Bug #4 修复: 不再调用 commission API (上面已本地计算)
                    #   funding API 仍需要查询, 因为它是按时间段对所有活跃仓位扣收的,
                    #   无法完全本地还原. 但当同 symbol 多组合并行时, 要按 qty 比例分摊.
                    _realized_funding = await self._get_binance_realized_funding_usd(
                        state.binance_future_symbol, _start_ms, _end_ms)
                    if _realized_funding is not None:
                        # 🌟 Plan Bug #4 修复: 计算同 symbol 同方向活跃组合总 qty, 按比例分摊 funding
                        # API 返回 symbol 级净仓位的 funding 总额，多空方向混合时不可按比例拆分
                        _total_bn_qty = Decimal(str(_funding_qty_ref)) if _funding_qty_ref else Decimal('0')
                        _has_opposite_direction = False
                        for _st in self.arbitrage_states.values():
                            if _st is state:
                                continue
                            if _st.binance_future_symbol != state.binance_future_symbol:
                                continue
                            if _st.state not in ('position_open', 'executing', 'exiting'):
                                continue
                            if _st.strategy_type != state.strategy_type:
                                _has_opposite_direction = True
                                continue
                            _total_bn_qty += _st.binance_filled_qty
                        if _has_opposite_direction:
                            _realized_funding = None
                            logger.info(
                                f"{log_prefix} funding: 同 symbol 存在反向组合, "
                                f"API 净额不可按方向拆分, 降级到费率估算")
                        elif (_total_bn_qty > 0 and _funding_qty_ref
                                and Decimal(str(_funding_qty_ref)) < _total_bn_qty):
                            _ratio = float(Decimal(str(_funding_qty_ref)) / _total_bn_qty)
                            funding_net_usd = float(_realized_funding) * _ratio
                            _funding_source = "realized_prorata"
                            logger.info(
                                f"{log_prefix} funding 按 qty 分摊: 本组合={_funding_qty_ref}/总={_total_bn_qty} "
                                f"(ratio={_ratio:.3f}), 分摊后={funding_net_usd:+.2f} USD")
                        else:
                            funding_net_usd = float(_realized_funding)
                            _funding_source = "realized"

                    if _funding_source == "none":
                        # API 不可用 → 降级费率估算
                        _hold_hours = max((time.time() - _entry_ts) / 3600, 0)
                        _fund_rate = self.binance_ws.funding_rates.get(
                            state.binance_future_symbol, Decimal('0')) if self.binance_ws else Decimal('0')
                        _fund_pos_val = bn_entry * _funding_qty_ref
                        _fund_raw = float(binance_futures.BinanceFeeCalculator.estimate_funding_cost_usdt(
                            _fund_pos_val, _fund_rate, _hold_hours))
                        # 方向: rate>0 → 多付空收; rate<0 → 空付多收
                        if state.strategy_type == 'sell_future_buy_synthetic':
                            # 空头(sell_future): rate>0 → 空头从多头收钱(正值=收入)
                            #                   rate<0 → 空头付钱给多头(负值=支出)
                            funding_net_usd = _fund_raw
                        else:
                            # 多头(buy_future): rate>0 → 多头付钱给空头(负值=支出)
                            #                  rate<0 → 多头从空头收钱(正值=收入)
                            funding_net_usd = -_fund_raw
                        _funding_source = "estimated"
                    state.accumulated_funding = Decimal(str(funding_net_usd))
                except Exception as _fe:
                    logger.info(f"{log_prefix} funding 估算异常: {_fe}")
                    _funding_source = "error"

            net_pnl_usd = deribit_pnl_usd + float(bn_pnl_usdt) - open_fee_usd - close_fee_usd - _delivery_fee_usd + funding_net_usd
        except Exception as e:
            logger.warning(f"{log_prefix} 交割 P&L 计算异常: {e}")
            # 避免异常后继续写入 0 值 P&L/记录 并错误结束状态，留给下轮 monitor 重试
            state.last_update = time.time()
            try:
                await self._save_state_to_redis(state)
            except Exception as _save_err:
                logger.info(f"{log_prefix} 交割 P&L 异常分支持久化失败: {_save_err}")
            return

        # ===== 5. 日志输出 =====
        pnl_sign = '+' if net_pnl_usd >= 0 else ''
        pnl_icon = '💰' if net_pnl_usd >= 0 else '📉'

        logger.info(
            f"{log_prefix}=== 交割结算确认 ===\n"
            f"{log_prefix}  合约组: {combination.get('call', '?')} / {combination.get('put', '?')}\n"
            f"{log_prefix}  数量: {entry_amount} {self.target_currency} | 策略: {state.strategy_type}\n"
            f"{log_prefix}  开仓价: F={f_entry} C={c_entry:.5f} P={p_entry:.5f}\n"
            f"{log_prefix}  结算价: BTC≈${settlement_price:.0f} | C结算={call_settlement:.5f} P结算={put_settlement:.5f}\n"
            f"{log_prefix}  Binance: 入场={bn_entry:.2f} 平仓={bn_close_price:.2f} 数量={bn_qty}\n"
            f"{log_prefix}  手续费: 开仓={open_fee_usd:.2f} 平仓={close_fee_usd:.2f} 交割={_delivery_fee_usd:.2f} "
            f"funding={funding_net_usd:+.2f} USD (fee={_fee_source}, funding={_funding_source})\n"
            f"{log_prefix}  {pnl_icon} 往返净利润: {pnl_sign}{net_pnl_usd:.2f} USD\n"
            f"{log_prefix}=== 交割结算完成 ==="
        )

        # ===== 6. 交易记录 =====
        actual_f_rec = bn_close_price if bn_close_price > 0 else settlement_price
        close_record = {
            '订单ID': state.combo_id,
            '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            '策略方向': state.strategy_type or '',
            '到期日': expiry, '行权价': float(strike),
            '标的': self.target_currency,
            '期权数量': float(entry_amount),
            '期货面值(USD)': float(future_size_usd),
            '模拟_Future价格': 0,
            '实际_Future均价': float(actual_f_rec),
            '模拟_Call价格': 0,
            '实际_Call均价': float(call_settlement),
            '模拟_Put价格': 0,
            '实际_Put均价': float(put_settlement),
            '模拟_手续费(USD)': 0,
            # 🌟 P2 修复: 手续费字段不应混入 funding（funding 已在 '已实现funding(USD)' 单独记录）
            '实际_手续费(USD)': round(open_fee_usd + close_fee_usd + _delivery_fee_usd, 4),
            '开仓手续费(USD)': round(open_fee_usd, 4),
            '预估结算手续费(USD)': round(close_fee_usd + _delivery_fee_usd, 4),
            '已实现funding(USD)': round(funding_net_usd, 4),
            '模拟_净利润(USD)': 0,
            '实际_净利润(USD)': round(net_pnl_usd, 4),
            '滑点与偏差损失(USD)': 0,
            # 🌟 P1-17 + P2 审计补全: 订单 ID 完整追溯链
            #   Call_ID/Put_ID: 开仓时的 Deribit 期权单 ID (到期自动结算, 无平仓单)
            #   Future_ID: 格式 "{开仓ID}|{平仓ID1,平仓ID2,...}"
            #              - 开仓 ID 为空时写 UNCONFIRMED
            #              - TWAP 多分片平仓时平仓部分以 ',' 内部分隔
            #              - 仅开仓无平仓(如交割无对冲)时只写开仓 ID
            'Call_ID': getattr(state, 'call_order_id', '') or 'UNCONFIRMED',
            'Put_ID': getattr(state, 'put_order_id', '') or 'UNCONFIRMED',
            'Future_ID': self._format_future_id(state),
            '交易类型': '交割结算', '平仓原因': '到期交割',
            '实际对冲关闭时间': time.strftime(
                '%Y-%m-%d %H:%M:%S',
                time.localtime(float(getattr(state, '_hedge_close_completed_ts', 0.0)) or time.time())),
        }
        _inserted = await self._persist_terminal_record(close_record)
        # 🌟 C 修复: 标记记录已写, 防止二次进入 _handle_delivery_settlement 重复记录
        # 同步落盘成功后才设标志 — _persist_terminal_record 失败会抛异常，标志不会被设置
        state._delivery_csv_written = True
        # 日损账本: 只在新插入时累加，幂等重试不重复计入
        if _inserted:
            try:
                from datetime import datetime, timezone
                _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                if getattr(self, '_daily_loss_date', None) != _today:
                    self._daily_loss_date = _today
                    self._daily_realized_pnl = 0.0
                    self._daily_loss_triggered = False
                self._daily_realized_pnl += float(net_pnl_usd)
                logger.info(f"📅 [日损追踪] 交割结算累加, 今日净盈亏: ${self._daily_realized_pnl:+.2f}")
                asyncio.create_task(self._save_daily_pnl_to_redis())
            except Exception:
                pass
        # 立刻持久化标记，避免"记录已写但进程崩溃"后重启重复落盘
        try:
            await self._save_state_to_redis(state)
        except Exception as _save_err:
            logger.warning(f"{log_prefix} 交割落盘标记持久化失败(将继续流程): {_save_err}")

        # ===== 7. Telegram 通知 =====
        _bn_info = f"Binance 平仓: {bn_close_price:.2f} × {bn_qty}\n" if bn_qty > 0 else ""
        asyncio.create_task(tg_notifier.send_async(
            f"📦 {log_prefix} 交割结算完成\n"
            f"策略: {state.strategy_type}\n"
            f"结算价: ${settlement_price:.0f}\n"
            f"{_bn_info}"
            f"{pnl_icon} 净利润: {pnl_sign}{net_pnl_usd:.2f} USD\n"
            f"手续费: {open_fee_usd + close_fee_usd + _delivery_fee_usd - funding_net_usd:.2f} USD (funding {funding_net_usd:+.2f})"))

        # ===== 8. 清理状态 =====
        state.state = 'exited'
        state.last_update = time.time()
        await self._delete_state_from_redis(expiry, strike)
        self.position_locks.discard(state.expiry_strike)
        self._bn_mark_missing_since.pop(state.expiry_strike, None)
        self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
        self._broken_combo_first_seen.pop(state.expiry_strike, None)
        self._broken_combo_handling.discard(state.expiry_strike)
        self._combo_closing_locks.pop(state.expiry_strike, None)
        if hasattr(self, '_exit_attempt_notified'):
            self._exit_attempt_notified.discard(state.expiry_strike)
        if hasattr(self, '_exit_fail_notified'):
            self._exit_fail_notified.discard(state.expiry_strike)

        # ===== 9. 交割释放保证金后自动尝试恢复交易 =====
        asyncio.create_task(self._try_auto_resume_after_delivery(log_prefix))

    async def _try_auto_resume_after_delivery(self, log_prefix: str = ""):
        """交割结算释放保证金后，检查是否应自动解除保证金不足暂停"""
        _margin_pauses = [p for p in ("Binance保证金不足", "Deribit保证金不足") if self._has_pause(p)]
        if not _margin_pauses:
            return
        try:
            await asyncio.sleep(3)
            for _mp in _margin_pauses:
                if "Binance" in _mp:
                    if not self.binance_ws:
                        continue
                    _acct = await self.binance_ws.get_account_info()
                    if not _acct:
                        continue
                    _avail = float(_acct.get('availableBalance', 0))
                    _bn_price = float(self.binance_ws.mark_prices.get('BTCUSDT', 0) or
                                      self.binance_ws.last_prices.get('BTCUSDT', 0))
                    if _bn_price <= 0:
                        _bn_price = 75000.0
                    _needed = float(self.trade_amount) * _bn_price * 0.05 * 2.0
                    if _avail >= _needed:
                        self._remove_pause(_mp)
                        logger.info(f"{log_prefix}✅ 交割后 Binance 保证金已恢复 (可用={_avail:.2f} >= 所需={_needed:.2f})，自动解除暂停")
                        asyncio.create_task(tg_notifier.send_async(
                            f"✅ 交割后 Binance 保证金已恢复\n"
                            f"可用: {_avail:.2f} USDT\n所需: {_needed:.2f} USDT\n已自动恢复开仓"))
                    else:
                        logger.info(f"{log_prefix}⚠️ 交割后 Binance 保证金仍不足 (可用={_avail:.2f} < 所需={_needed:.2f})")

                if "Deribit" in _mp:
                    try:
                        _dr = await self.client.send_request({
                            "jsonrpc": "2.0",
                            "id": self.client._get_next_request_id(),
                            "method": "private/get_account_summary",
                            "params": {"currency": self.target_currency}
                        }, is_private=True)
                        _dr_avail = float(_dr.get('result', {}).get('available_funds', 0))
                        if _dr_avail >= 0.01:
                            self._remove_pause(_mp)
                            logger.info(f"{log_prefix}✅ 交割后 Deribit 保证金已恢复 (可用={_dr_avail:.4f})，自动解除暂停")
                            asyncio.create_task(tg_notifier.send_async(
                                f"✅ 交割后 Deribit 保证金已恢复\n"
                                f"可用: {_dr_avail:.4f} {self.target_currency}\n已自动恢复开仓"))
                        else:
                            logger.info(f"{log_prefix}⚠️ 交割后 Deribit 保证金仍不足 (可用={_dr_avail:.4f})")
                    except Exception:
                        pass
        except Exception as e:
            logger.info(f"{log_prefix} 交割后保证金自动恢复检查异常: {e}")

    async def _hedge_fail_auto_recover(self, log_prefix: str = ""):
        """对冲失败（非保证金原因）后等待60秒，检查问题是否恢复，自动解除暂停"""
        if getattr(self, '_hedge_auto_recover_running', False):
            logger.info("_hedge_fail_auto_recover 已在运行中，跳过重复触发")
            return
        self._hedge_auto_recover_running = True
        try:
            await self._hedge_fail_auto_recover_inner(log_prefix)
        finally:
            self._hedge_auto_recover_running = False

    async def _hedge_fail_auto_recover_inner(self, log_prefix: str = ""):
        await asyncio.sleep(60)
        _reason = "Binance对冲临时失败"
        if not self._has_pause(_reason):
            return
        # 检查 Binance 连接 + 余额是否恢复
        _recovered = False
        try:
            if self.binance_ws and self.binance_connected:
                _acct = await self.binance_ws.get_account_info()
                if _acct and Decimal(str(_acct.get('availableBalance', '0'))) > 0:
                    _recovered = True
        except Exception:
            pass
        if _recovered:
            self._remove_pause(_reason)
            logger.info(f"{log_prefix}✅ Binance 对冲临时失败已恢复，自动解除暂停")
            asyncio.create_task(tg_notifier.send_async(
                f"✅ Binance 对冲失败已自动恢复（60秒冷却后确认连接+余额正常）"))
        else:
            logger.warning(f"{log_prefix}⚠️ 60秒后 Binance 仍未恢复，保持暂停，请检查连接或余额")
            asyncio.create_task(tg_notifier.send_error_async(
                f"⚠️ Binance 对冲失败60秒后仍未恢复\n请检查连接状态或发送 /start 手动恢复",
                "hedge_fail_no_recover"))

    async def emergency_liquidate_all(self, full_stop: bool = True) -> List[str]:
        """
        🚨 一键清仓：对当前标的币种的所有持仓执行暴力平仓
        逻辑：
        - full_stop=True: 停止扫描并清仓（/stop_all 路径）
        - full_stop=False: 仅执行清仓，保留监控循环继续处理残仓（全局硬止损路径）
        """
        results = []
        log_prefix = "【一键清仓】"

        if self._is_deribit_settlement_core_window():
            self._add_pause("结算窗口")
            if full_stop:
                self.running = False
                self._add_pause("紧急清仓")
                self.trade_executor.emergency_stop = True
                self._pending_stop_all_after_settlement = True
                try:
                    await self.client.cancel_all_orders(self.target_currency)
                except Exception as _ce:
                    logger.error(f"{log_prefix} 结算核心窗口撤单异常: {_ce}")
                msg = "Deribit core settlement window active; 已停止扫描并撤单，平仓延后到窗口结束后执行"
            else:
                msg = "Deribit core settlement window active; 自动清仓已延后，窗口结束后重新评估"
            logger.warning(f"{log_prefix} ⏸️ {msg}")
            return [msg]

        # 0. 第零步：阻止新的套利执行
        if full_stop:
            self.running = False
            self._add_pause("紧急清仓")
        else:
            # 全局硬止损路径：不停止监控循环，只暂停开仓
            self.running = True
        self.trade_executor.emergency_stop = True  # 让 Maker 等待循环立即退出
        logger.warning(f"{log_prefix} 已设置全局停止标志，等待所有执行中的套利任务退出...")

        # 等待所有正在执行的套利任务完成（最多等 5 秒）
        for i in range(10):
            if not self.processing_opportunities:
                break
            logger.info(f"{log_prefix} 仍有 {len(self.processing_opportunities)} 个任务执行中，等待退出... ({i+1}/10)")
            await asyncio.sleep(0.5)

        if self.processing_opportunities:
            logger.warning(f"{log_prefix} 超时！仍有 {len(self.processing_opportunities)} 个任务未退出，强制继续清仓")

        # 1. 第一步：撤回所有当前币种的挂单
        logger.warning(f"{log_prefix} 正在撤销所有 {self.target_currency} 活跃订单...")
        await self.client.cancel_all_orders(self.target_currency)
        await asyncio.sleep(1.0)  # 等待交易所状态同步

        # 2. 第二步：同步最新持仓真相
        positions = await self.client.get_positions(self.target_currency)
        _deribit_positions_known = bool(getattr(self.client, '_last_positions_refresh_ok', False))
        _fresh_deribit_positions = {
            p.instrument_name: p for p in positions
            if getattr(p, 'size', Decimal('0')) != 0
        } if _deribit_positions_known else {}
        if not positions and _deribit_positions_known:
            positions = []
            results.append("Deribit 当前无持仓。")
        elif not _deribit_positions_known:
            results.append("Deribit 持仓查询未知，保守保留状态。")

        tasks = []
        for p in positions:
            if p.size == 0:
                continue

            side = 'sell' if p.size > 0 else 'buy'
            amount = abs(p.size)
            inst = p.instrument_name

            # 区分平仓策略
            if self.client._is_future_instrument(inst):
                # 期货直接市价单
                logger.info(f"{log_prefix} 期货强平: {inst} {side} {amount}")
                tasks.append(self.client.place_order(
                    inst, amount, side, 'market', reduce_only=True, label="stop_all_f"
                ))
                results.append(f"期货 {inst}: 发起市价平仓 {amount}")

            elif self.client._is_option_instrument(inst):
                # 期权使用 Deribit close_position API（自动计算合理穿透价，避免 price_too_low/high）
                logger.info(f"{log_prefix} 期权强平: {inst} {side} {amount}")
                try:
                    close_resp = await self.client.send_request({
                        "jsonrpc": "2.0",
                        "id": self.client._get_next_request_id(),
                        "method": "private/close_position",
                        "params": {
                            "instrument_name": inst,
                            "type": "market"
                        }
                    }, is_private=True)
                    if 'error' in close_resp:
                        err_msg = close_resp['error'].get('message', '未知')
                        logger.error(f"{log_prefix} close_position 失败: {inst} -> {err_msg}")
                        # 降级方案：优先用交易所允许的 min/max_price（与 _close_option_position 一致）
                        ticker = self.client.tickers.get(inst)
                        if ticker:
                            if side == 'sell':
                                # 卖出：用最低允许价，保证立即成交
                                if hasattr(ticker, 'min_price') and ticker.min_price > 0:
                                    px = ticker.min_price
                                else:
                                    px = max(ticker.bid * Decimal('0.5'), Decimal('0.0001'))
                            else:
                                # 买入：用最高允许价，保证立即成交
                                if hasattr(ticker, 'max_price') and ticker.max_price > 0:
                                    px = ticker.max_price
                                else:
                                    px = ticker.ask * Decimal('2') if ticker.ask > 0 else Decimal('1.0')
                            px = self.client._adjust_to_tick_size(px, self._get_dynamic_tick(px))
                            tasks.append(self.client.place_order(
                                inst, amount, side, 'limit', price=px,
                                reduce_only=True, time_in_force="good_til_cancelled", label="stop_all_o"
                            ))
                        results.append(f"期权 {inst}: close_position 失败，降级穿透限价")
                    else:
                        results.append(f"期权 {inst}: close_position 已发射")
                except Exception as e:
                    logger.error(f"{log_prefix} close_position 异常: {inst} -> {e}")
                    results.append(f"期权 {inst}: 平仓异常 {str(e)[:50]}")

        # 3. 执行并发平仓（Deribit 期货市价单）
        if tasks:
            await asyncio.gather(*tasks)

        # 3.5 Binance 撤单 + 平仓
        # 如果 WS 已连接，直接用现有连接；否则用 REST 临时创建会话
        _bn_rest_session = None
        _bn_auth = getattr(self, 'binance_auth', None)
        _sa_perp = self.binance_matcher.perpetual_symbol if hasattr(self, 'binance_matcher') else 'BTCUSDT'
        try:
            if self.binance_ws and self.binance_connected and self.binance_executor:
                # ===== 路径 A: WS 已连接，用现有连接 =====
                _bn_rest = self.binance_executor.ws_client
                _bn_positions = self.binance_ws.positions
                _bn_executor = self.binance_executor
            elif _bn_auth:
                # ===== 路径 B: WS 未连接但 auth 已配置，用 REST 临时查询 =====
                logger.warning(f"{log_prefix} Binance WS 未连接，使用 REST API 直接清仓...")
                _bn_rest_session = aiohttp.ClientSession(
                    headers=_bn_auth.headers, timeout=aiohttp.ClientTimeout(total=10))
                _bn_rest = type('_TempRest', (), {
                    '_rest_request': lambda self_inner, method, path, params=None, signed=False: \
                        self._binance_rest_fallback(_bn_auth, _bn_rest_session, method, path, params, signed)
                })()
                _bn_positions = None  # 需要通过 REST 查询
                _bn_executor = None
            else:
                _bn_rest = None
                _bn_executor = None
                _bn_positions = None

            if _bn_rest:
                # 撤销 Binance 所有挂单
                try:
                    logger.warning(f"{log_prefix} 正在撤销 Binance 所有活跃订单...")
                    await _bn_rest._rest_request("DELETE", "/fapi/v1/allOpenOrders",
                                                  {"symbol": _sa_perp}, signed=True)
                    results.append("Binance 挂单: 已全部撤销")
                except Exception as e:
                    logger.error(f"{log_prefix} Binance 撤单失败: {e}")
                    results.append(f"Binance 撤单: 失败 {str(e)[:50]}")

                # 查询 Binance 持仓（WS 已连接用缓存，否则 REST 查询）
                _bn_pos_list = []
                if _bn_positions:
                    if self.binance_dual_side_mode and self.binance_ws and getattr(self.binance_ws, "positions_by_side", None):
                        for (bn_sym, ps), bn_pos in list(self.binance_ws.positions_by_side.items()):
                            if bn_pos.quantity > 0:
                                _bn_pos_list.append((bn_sym, ps, bn_pos.quantity, ps))
                    else:
                        for bn_sym, bn_pos in list(_bn_positions.items()):
                            if bn_pos.quantity > 0:
                                _bn_pos_list.append((bn_sym, bn_pos.side, bn_pos.quantity, ""))
                else:
                    # REST 查询持仓
                    try:
                        _pos_data = await _bn_rest._rest_request("GET", "/fapi/v2/positionRisk",
                                                                  {"symbol": _sa_perp}, signed=True)
                        if _pos_data:
                            for p in (_pos_data if isinstance(_pos_data, list) else [_pos_data]):
                                _amt = abs(Decimal(str(p.get('positionAmt', '0'))))
                                if _amt > 0:
                                    _ps = str(p.get('positionSide', 'BOTH')).upper()
                                    if _ps in ("LONG", "SHORT"):
                                        _side = _ps
                                    else:
                                        _side = "LONG" if Decimal(str(p.get('positionAmt', '0'))) > 0 else "SHORT"
                                        _ps = ""
                                    _bn_pos_list.append((p.get('symbol', _sa_perp), _side, _amt, _ps))
                    except Exception as e:
                        logger.error(f"{log_prefix} Binance 查询持仓失败: {e}")
                        results.append(f"Binance 持仓查询: 失败 {str(e)[:50]}")

                # 平掉 Binance 持仓
                for bn_sym, bn_side, bn_qty, bn_ps in _bn_pos_list:
                    close_side = "SELL" if bn_side == "LONG" else "BUY"
                    logger.warning(f"{log_prefix} Binance 强平: {bn_sym} {close_side} {bn_qty} ps={bn_ps or 'AUTO'}")
                    try:
                        if _bn_executor:
                            # 路径 A: executor (WS 可用), 内部已有 newClientOrderId + -5022 恢复
                            _close_result = await _bn_executor.place_market_order(
                                bn_sym, close_side, bn_qty, reduce_only=True, position_side=bn_ps or None)
                        else:
                            # 路径 B: REST 降级 - 🌟 P1-8/P1-12 补齐 client_oid + -5022 恢复
                            import time as _t, random as _r
                            _stopall_cid = f"stopall_{int(_t.time() * 1000)}_{_r.randint(1000, 9999)}"
                            _close_params = {
                                "symbol": bn_sym, "side": close_side, "type": "MARKET",
                                "quantity": str(bn_qty),
                                "newClientOrderId": _stopall_cid,
                                "newOrderRespType": "RESULT"
                            }
                            if bn_ps in ("LONG", "SHORT", "BOTH"):
                                _close_params["positionSide"] = bn_ps
                            if bn_ps not in ("LONG", "SHORT"):
                                _close_params["reduceOnly"] = "true"
                            _close_params = _bn_auth.sign(_close_params)
                            _close_result = await _bn_rest._rest_request(
                                "POST", "/fapi/v1/order", _close_params, signed=False)
                            # -5022 Duplicate 幂等恢复
                            if isinstance(_close_result, dict) and _close_result.get('code') in (-5022, "-5022"):
                                logger.warning(f"[一键清仓REST] -5022 Duplicate, 查询 cid={_stopall_cid}")
                                _q_params = _bn_auth.sign({"symbol": bn_sym, "origClientOrderId": _stopall_cid})
                                _q_result = await _bn_rest._rest_request(
                                    "GET", "/fapi/v1/order", _q_params, signed=False)
                                if isinstance(_q_result, dict) and _q_result.get('orderId'):
                                    _close_result = _q_result
                        if _close_result and _close_result.get('status') == 'FILLED':
                            _avg = _close_result.get('avgPrice', '?')
                            results.append(f"Binance {bn_sym}: {close_side} {bn_qty} @ {_avg}")
                        else:
                            await asyncio.sleep(1)
                            results.append(f"Binance {bn_sym}: 平仓已发送 (状态={_close_result.get('status') if _close_result else 'None'})")
                    except Exception as e:
                        logger.error(f"{log_prefix} Binance {bn_sym} 平仓失败: {e}")
                        results.append(f"Binance {bn_sym}: 平仓失败 {str(e)[:50]}")
        finally:
            if _bn_rest_session:
                await _bn_rest_session.close()

        # 4. 同步最新仓位快照，再写记录并更新状态机
        try:
            await self.client.get_positions(self.target_currency, silent=True)
        except Exception:
            pass

        # 写入平仓记录（含 P&L 估算）+ 按"真实残仓"决定状态
        _has_any_residual = False
        for state in list(self.arbitrage_states.values()):
            if state.state in ('position_open', 'executing', 'exiting'):
                expiry, strike = state.expiry_strike
                # 作用域初始化: 残仓判定变量在 try 块外也要可读 (try 异常时 state 决策用)
                _has_residual = False
                _residual_msgs = []
                _bn_unknown = False
                try:
                    _sa_entry = state.entry_prices or {}
                    _sa_f_entry = _sa_entry.get('future', Decimal('0'))
                    _sa_c_entry = _sa_entry.get('call', Decimal('0'))
                    _sa_p_entry = _sa_entry.get('put', Decimal('0'))
                    _sa_amount = state.entry_amount or self.trade_amount

                    # 用当前盘口估算平仓价
                    _sa_f_exit = Decimal('0')
                    _sa_bn_ob = self.binance_ws.order_books.get(_sa_perp) if self.binance_ws else None
                    _sa_bn_ob_age = time.time() - _sa_bn_ob.update_time if _sa_bn_ob and getattr(_sa_bn_ob, 'update_time', 0) else float('inf')
                    if _sa_bn_ob and _sa_bn_ob.mid_price is not None and _sa_bn_ob.mid_price > 0 and _sa_bn_ob_age <= 30:
                        _sa_f_exit = _sa_bn_ob.mid_price
                    elif _sa_bn_ob and _sa_bn_ob.mid_price is not None and _sa_bn_ob.mid_price > 0:
                        logger.warning(
                            f"{log_prefix} stop_all PnL 估算跳过过期 Binance 盘口: {_sa_bn_ob_age:.1f}s")

                    # 估算 P&L (开+平双边费用)
                    _sa_pnl = 0.0
                    _sa_fee = 0.0
                    try:
                        _sa_ref = _sa_f_exit if _sa_f_exit > 0 else _sa_f_entry
                        if _sa_ref > 0 and _sa_c_entry > 0 and _sa_p_entry > 0:
                            # Binance 期货 P&L
                            _sa_bn_qty = state.binance_filled_qty or _sa_amount
                            if _sa_f_exit > 0 and state.binance_entry_price > 0:
                                if state.strategy_type == 'sell_future_buy_synthetic':
                                    _sa_pnl += float((state.binance_entry_price - _sa_f_exit) * _sa_bn_qty)
                                else:
                                    _sa_pnl += float((_sa_f_exit - state.binance_entry_price) * _sa_bn_qty)
                            # 期权按市价估算(close_position 已用市价)
                            _combo = self.arbitrage_combinations.get((expiry, Decimal(str(strike))))
                            _sa_c_exit = Decimal('0')
                            _sa_p_exit = Decimal('0')
                            if _combo:
                                _sa_c_tk = self.client.tickers.get(_combo.get('call', ''))
                                _sa_p_tk = self.client.tickers.get(_combo.get('put', ''))
                                _sa_c_exit = _sa_c_tk.mid_price if _sa_c_tk and _sa_c_tk.mid_price > 0 else Decimal('0')
                                _sa_p_exit = _sa_p_tk.mid_price if _sa_p_tk and _sa_p_tk.mid_price > 0 else Decimal('0')
                                if _sa_c_exit > 0 and _sa_p_exit > 0:
                                    if state.strategy_type == 'sell_future_buy_synthetic':
                                        _sa_pnl += float((_sa_c_exit - _sa_c_entry - _sa_p_exit + _sa_p_entry) * _sa_amount * _sa_ref)
                                    else:
                                        _sa_pnl += float((_sa_c_entry - _sa_c_exit + _sa_p_exit - _sa_p_entry) * _sa_amount * _sa_ref)
                            # 动态费率估算（开+平双边）
                            _sa_open_fee_usd = Decimal('0')
                            _sa_close_fee_usd = Decimal('0')
                            _sa_fc = self.trade_executor.fee_calculator
                            _sa_bn_entry = state.binance_entry_price if state.binance_entry_price > 0 else _sa_f_entry
                            _sa_bn_close = _sa_f_exit if _sa_f_exit > 0 else _sa_ref

                            if _sa_f_entry > 0 and _sa_c_entry > 0 and _sa_p_entry > 0:
                                _sa_oc_btc = _sa_fc.calculate_option_fee(
                                    _sa_f_entry, _sa_c_entry, _sa_amount, is_taker=True)
                                _sa_op_btc = _sa_fc.calculate_option_fee(
                                    _sa_f_entry, _sa_p_entry, _sa_amount, is_taker=True)
                                _sa_open_fee_usd += (_sa_oc_btc + _sa_op_btc) * _sa_f_entry
                            if _sa_bn_entry > 0 and _sa_bn_qty > 0:
                                _sa_open_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                                    _sa_bn_entry, _sa_bn_qty, is_taker=True)

                            _sa_c_close_ref = _sa_c_exit if _sa_c_exit > 0 else _sa_c_entry
                            _sa_p_close_ref = _sa_p_exit if _sa_p_exit > 0 else _sa_p_entry
                            if _sa_bn_close > 0 and _sa_c_close_ref > 0 and _sa_p_close_ref > 0:
                                _sa_cc_btc = _sa_fc.calculate_option_fee(
                                    _sa_bn_close, _sa_c_close_ref, _sa_amount, is_taker=True)
                                _sa_cp_btc = _sa_fc.calculate_option_fee(
                                    _sa_bn_close, _sa_p_close_ref, _sa_amount, is_taker=True)
                                _sa_close_fee_usd += (_sa_cc_btc + _sa_cp_btc) * _sa_bn_close
                            if _sa_bn_close > 0 and _sa_bn_qty > 0:
                                _sa_close_fee_usd += self.trade_executor._calculate_binance_fee_usdt(
                                    _sa_bn_close, _sa_bn_qty, is_taker=True)

                            _sa_fee = float(_sa_open_fee_usd + _sa_close_fee_usd)
                            _sa_pnl -= _sa_fee
                    except Exception:
                        pass

                    # 🌟 P1 回归修复: 先验残仓 → 再写记录
                    # 原逻辑先落 "紧急清仓" 记录, 再查残仓; 若残仓 → state 回滚 position_open
                    #    下次 /stop_all 会再次落盘 → 审计链出现重复的"已清仓"记录
                    # 改为: 在落盘前先核验残仓, 根据真实结果决定写入内容
                    #       同时加 _stop_all_record_written 标志防同一 state 多次重入时重复写
                    _has_residual = False
                    _residual_msgs = []
                    _combo = self.arbitrage_combinations.get(state.expiry_strike)
                    if _combo:
                        _deribit_position_source = (
                            _fresh_deribit_positions
                            if _deribit_positions_known else self.client.positions
                        )
                        _f_now = _deribit_position_source.get(_combo['future'])
                        _c_now = _deribit_position_source.get(_combo['call'])
                        _p_now = _deribit_position_source.get(_combo['put'])
                        if _f_now and _f_now.size != 0:
                            _has_residual = True
                            _residual_msgs.append(f"Deribit期货 {_combo['future']}={_f_now.size}")
                        if _c_now and _c_now.size != 0:
                            _has_residual = True
                            _residual_msgs.append(f"Call {_combo['call']}={_c_now.size}")
                        if _p_now and _p_now.size != 0:
                            _has_residual = True
                            _residual_msgs.append(f"Put {_combo['put']}={_p_now.size}")

                    _bn_unknown = False
                    if state.binance_future_symbol:
                        _bn_qty, _, _, _bn_known = await self._get_binance_actual_position(
                            state.binance_future_symbol, state.binance_position_side)
                        if not _bn_known:
                            _bn_unknown = True
                            _has_residual = True
                            _residual_msgs.append(f"Binance {state.binance_future_symbol}=unknown")
                        elif _bn_qty > Decimal('0.0001'):
                            _has_residual = True
                            _residual_msgs.append(f"Binance {state.binance_future_symbol}={_bn_qty}")

                    # 根据真实残仓状态组装记录内容 (平仓原因反映实际结果)
                    if _has_residual:
                        _stop_all_reason = f'stop_all (部分清仓, 残余: {", ".join(_residual_msgs)})'
                        _stop_all_type = '紧急清仓(部分)'
                    else:
                        _stop_all_reason = 'stop_all'
                        _stop_all_type = '紧急清仓'
                    # 幂等写入: 同一 state 在反复 /stop_all 时只写一次记录
                    if getattr(state, '_stop_all_record_written', False):
                        logger.info(
                            f"{log_prefix} [{expiry}-{strike}] 跳过重复写入 (stop_all 已落盘过)")
                    else:
                        close_record = {
                            '订单ID': state.combo_id,
                            '成交时间': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                            '策略方向': state.strategy_type or '',
                            '到期日': expiry, '行权价': float(strike),
                            '标的': self.target_currency,
                            '期权数量': float(_sa_amount),
                            '期货面值(USD)': float(state.future_size_usd) if state.future_size_usd else 0,
                            '模拟_Future价格': float(_sa_f_entry), '实际_Future均价': float(_sa_f_exit),
                            '模拟_Call价格': float(_sa_c_entry), '实际_Call均价': 0,
                            '模拟_Put价格': float(_sa_p_entry), '实际_Put均价': 0,
                            '模拟_手续费(USD)': 0, '实际_手续费(USD)': round(_sa_fee, 4),
                            '模拟_净利润(USD)': 0, '实际_净利润(USD)': round(_sa_pnl, 4),
                            '滑点与偏差损失(USD)': 0,
                            # 🌟 P1-17 + P2: 订单 ID 审计补齐
                            'Call_ID': getattr(state, 'call_order_id', '') or 'UNCONFIRMED',
                            'Put_ID': getattr(state, 'put_order_id', '') or 'UNCONFIRMED',
                            'Future_ID': self._format_future_id(state),
                            '交易类型': _stop_all_type, '平仓原因': _stop_all_reason,
                            # stop_all 同步关闭，写入时间 ≈ 对冲关闭时间
                            '实际对冲关闭时间': time.strftime(
                                '%Y-%m-%d %H:%M:%S',
                                time.localtime(float(getattr(state, '_hedge_close_completed_ts', 0.0)) or time.time())),
                        }
                        _inserted = await self._persist_terminal_record(close_record)
                        state._stop_all_record_written = True
                        if _inserted:
                            try:
                                from datetime import datetime, timezone
                                _today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                                if getattr(self, '_daily_loss_date', None) != _today:
                                    self._daily_loss_date = _today
                                    self._daily_realized_pnl = 0.0
                                    self._daily_loss_triggered = False
                                self._daily_realized_pnl += float(_sa_pnl)
                                logger.info(f"📅 [日损追踪] stop_all累加, 今日净盈亏: ${self._daily_realized_pnl:+.2f}")
                                asyncio.create_task(self._save_daily_pnl_to_redis())
                            except Exception:
                                pass
                except Exception as e:
                    logger.error(f"{log_prefix} 记录写入失败 [{expiry}-{strike}]: {e}")
                    # 🛡️ 回归修复: try 块内含残仓检查 + 记录写入, 若其中 (尤其是残仓检查)
                    # 异常会被捕获 → _has_residual 保持初始 False → 误走 "exited" 分支
                    # 删 Redis + 放锁, 而真实残仓仍在。保守降级: 异常时视为"残仓未知"。
                    _has_residual = True
                    _bn_unknown = True
                    _residual_msgs.append(f"处理异常 (保守视为残仓): {str(e)[:80]}")

                if _has_residual:
                    _has_any_residual = True
                    state.state = 'position_open'
                    state.last_update = time.time()
                    self.position_locks.add(state.expiry_strike)
                    await self._save_state_to_redis(state)
                    self._add_pause("Binance残余仓位")
                    _msg = ", ".join(_residual_msgs) if _residual_msgs else "未知残仓"
                    logger.error(f"{log_prefix} [{expiry}-{strike}] 清仓后仍有残仓，保留状态机跟踪: {_msg}")
                    if _bn_unknown:
                        asyncio.create_task(tg_notifier.send_error_async(
                            f"🚨 [{expiry}-{strike}] stop_all 后 Binance 持仓状态未知\n"
                            f"残仓明细: {_msg}\n系统已保留状态并暂停开仓，请人工核对。",
                            "stop_all_residual_unknown"))
                    results.append(f"[{expiry}-{strike}] 残仓待处理: {_msg}")
                else:
                    state.state = 'exited'
                    state.last_update = time.time()
                    await self._delete_state_from_redis(state.expiry_strike[0], state.expiry_strike[1])
                    self.position_locks.discard(state.expiry_strike)
                    self._bn_mark_missing_since.pop(state.expiry_strike, None)
                    self._bn_mark_degraded_log_ts.pop(state.expiry_strike, None)
                    self._broken_combo_first_seen.pop(state.expiry_strike, None)
                    self._broken_combo_handling.discard(state.expiry_strike)
                    self._combo_closing_locks.pop(state.expiry_strike, None)

        # 5. 清理处理锁，防止残留
        self.processing_opportunities.clear()

        if full_stop:
            if _has_any_residual:
                self.running = False
                logger.warning(
                    f"{log_prefix} 检测到残仓，系统按 /stop_all 语义保持完全停止。"
                    f"请人工核对残仓，必要时窗口结束后再次发送 /stop_all。")
                asyncio.create_task(tg_notifier.send_error_async(
                    "⚠️ stop_all 后仍有残仓，系统保持完全停止。\n"
                    "请人工核对交易所持仓，必要时再次发送 /stop_all；确认安全后再 /start。",
                    "stop_all_residual_guard"))
            else:
                # 全平成功才停引擎
                self.running = False
                self._remove_pause("Binance残余仓位")
                self._remove_pause("破损组合")
                logger.warning(f"{log_prefix} 清仓完毕。系统已停止，需手动发送 start 恢复。")
        else:
            self.running = True
            logger.warning(f"{log_prefix} 清仓流程完成（全局硬止损路径），系统保持暂停开仓，监控继续运行。")

        return results
