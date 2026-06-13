"""telegram_handler.py — Telegram 通知器与命令处理 (从 binance-deribit.py 提取)"""
import logging
import asyncio
import time
import re
import os
import urllib.request
import urllib.parse
import config

import json
import traceback
from decimal import Decimal
from collections import defaultdict
import aiohttp

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Telegram 异步消息通知器与指令接收器 (带防轰炸机制)"""

    def __init__(self, token: str, chat_id: str, allowed_user_ids: str = ""):
        self.token = token
        self.chat_id = str(chat_id)
        self.api_url = f"https://api.telegram.org/bot{self.token}/"
        self.offset = 0
        self.engine = None  # 用于持有引擎引用以便控制
        # 🌟 P1-4.5: user_id 白名单 (逗号分隔, 留空则仅验证 chat_id)
        self._allowed_user_ids = set(
            _s.strip() for _s in str(allowed_user_ids or "").split(",") if _s.strip()
        )

        # ================= 新增：防轰炸拦截器 =================
        self.error_last_sent = {}  # 记录每种错误最后一次发送的时间戳
        self.error_cooldown = 300  # 相同类型的错误，冷却期为 300 秒 (5分钟)
        self.suppressed_counts = defaultdict(int)  # 记录冷却期内被静默折叠的错误次数
        self._command_lock = asyncio.Lock()  # 防止 start/stop/stop_all 等命令并发执行
        # 网络断联/恢复通知冷却（防刷屏）
        self._net_notify_cooldown = 300  # 5 分钟冷却
        self._net_last_disconnect = 0  # 上次发送断联通知的时间
        self._net_last_reconnect = 0   # 上次发送恢复通知的时间
        self._net_suppressed_disconnects = 0  # 冷却期内被折叠的断联次数
        self._net_suppressed_reconnects = 0   # 冷却期内被折叠的恢复次数

    def bind_engine(self, engine):
        self.engine = engine

    async def notify_network_disconnect(self):
        """网络断联记录（仅写日志，带冷却防刷屏）"""
        now = time.time()
        if now - self._net_last_disconnect < self._net_notify_cooldown:
            self._net_suppressed_disconnects += 1
            return
        # 构造消息：如果有被折叠的，附加汇总
        msg = "⚠️ 【网络断开】WebSocket 被动断开，交易已自动暂停，正在尝试重连..."
        if self._net_suppressed_disconnects > 0:
            msg += f"\n(此前 {self._net_suppressed_disconnects} 次断联通知已静默折叠)"
        self._net_suppressed_disconnects = 0
        self._net_last_disconnect = now
        logger.warning(msg)

    async def notify_network_reconnect(self):
        """网络恢复记录（仅写日志，带冷却防刷屏）"""
        now = time.time()
        if now - self._net_last_reconnect < self._net_notify_cooldown:
            self._net_suppressed_reconnects += 1
            return
        msg = ("✅ 【网络恢复正常】\n"
               "WebSocket 已成功重新连接！\n"
               "账户持仓已重新同步，盘口数据已恢复，系统继续执行套利扫描。")
        if self._net_suppressed_reconnects > 0:
            msg += f"\n(此前 {self._net_suppressed_reconnects} 次恢复通知已静默折叠)"
        self._net_suppressed_reconnects = 0
        self._net_last_reconnect = now
        logger.info(msg)

    def _send_sync(self, message: str):
        if not self.token or not self.chat_id: return
        try:
            data = urllib.parse.urlencode({'chat_id': self.chat_id, 'text': message}).encode('utf-8')
            req = urllib.request.Request(self.api_url + "sendMessage", data=data)
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            logger.info(f"Telegram 发送失败: {e}")

    async def send_async(self, message: str):
        if not self.token or not self.chat_id: return
        try:
            await asyncio.to_thread(self._send_sync, message)
        except Exception:
            pass

    async def send_error_async(self, message: str, error_type: str = "general_error"):
        """
        发送错误告警（自带强力防轰炸与折叠机制）
        :param message: 具体的报错信息
        :param error_type: 错误大类，用于分类冷却（如 "ws_error", "api_error"）
        """
        if not self.token or not self.chat_id: return

        current_time = time.time()
        last_time = self.error_last_sent.get(error_type, 0)

        # 判断是否跨过了冷却期
        if current_time - last_time >= self.error_cooldown:
            # 获取上一个周期内被我们静默拦截的次数
            suppressed_count = self.suppressed_counts.get(error_type, 0)
            suffix = f"\n\n*(注: 过去 {self.error_cooldown // 60} 分钟内，该错误被折叠拦截了 {suppressed_count} 次)*" if suppressed_count > 0 else ""

            final_msg = f"🚨 【系统异常】\n类型: {error_type}\n详情: {message}{suffix}"

            # 更新最后发送时间，并清零折叠计数器
            self.error_last_sent[error_type] = current_time
            self.suppressed_counts[error_type] = 0

            await self.send_async(final_msg)
        else:
            # 如果在冷却期内，不发 Telegram，只在后台默默 +1
            self.suppressed_counts[error_type] += 1

    def _get_updates_sync(self):
        try:
            url = f"{self.api_url}getUpdates?offset={self.offset}&timeout=10"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read().decode())
        except Exception:
            return None

    async def start_polling(self):
        """后台轮询监听指令"""
        if not self.token: return
        logger.info("Telegram 监听服务已启动...")
        while True:
            try:
                # 替换旧的 run_in_executor，消除 IDE 警告
                updates = await asyncio.to_thread(self._get_updates_sync)

                if updates and 'result' in updates:
                    for update in updates['result']:
                        self.offset = update['update_id'] + 1
                        message = update.get('message', {})
                        # chat_id 过滤 (防止其他人/群组发命令)
                        if str(message.get('chat', {}).get('id')) != self.chat_id:
                            continue
                        # 🌟 P1-4.5: user_id 白名单 (防止群聊中其他 member 发命令)
                        # 如果 TG_ALLOWED_USER_IDS 留空 → 只过滤 chat_id (兼容单用户场景)
                        # 如果设置了 → 必须是白名单中的 user_id 才能执行
                        _allowed_uids = getattr(self, '_allowed_user_ids', None)
                        if _allowed_uids:  # 非空集合才启用白名单
                            _from_uid = str(message.get('from', {}).get('id', ''))
                            if _from_uid and _from_uid not in _allowed_uids:
                                logger.warning(
                                    f"🚨 [Telegram] 拒绝非白名单 user_id: {_from_uid} "
                                    f"发送: {message.get('text', '')[:50]}")
                                continue

                        text = message.get('text', '').strip()
                        if text:
                            await self.handle_command(text)
            except Exception as e:
                await asyncio.sleep(2)
            await asyncio.sleep(0.5)

    async def handle_command(self, text: str):
        """处理收到的 Telegram 指令（加锁防止并发冲突）"""
        async with self._command_lock:
            await self._handle_command_impl(text)

    async def _handle_command_impl(self, text: str):
        """处理收到的 Telegram 指令（支持 /cmd 和 cmd 两种格式）"""
        # 统一去掉斜杠前缀，兼容 /stop 和 stop 两种输入
        if text.startswith('/'):
            text = text[1:]
        text_lower = text.lower()
        if not self.engine: return

        if text_lower == 't':
            # 动态获取当前运行的币种日志文件名
            current_coin = getattr(self.engine, 'target_currency', 'SYS') if self.engine else 'SYS'
            log_file = f"{current_coin}-log.txt"

            # 读取日志最后 15 行
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    last_lines = "".join(lines[-15:])
                    await self.send_async(f"📜 最新日志 ({log_file}):\n{last_lines}")
            except Exception as e:
                await self.send_async(f"读取日志文件 {log_file} 失败: {e}")

        elif text_lower == 'stop':
            self.engine._add_pause("手动stop")
            self.engine._manual_stop = True
            _stop_msgs = []
            # 1. 撤销 Deribit 所有挂单
            try:
                await self.engine.client.cancel_all_orders(self.engine.target_currency)
                _stop_msgs.append("✅ Deribit 挂单已撤销")
            except Exception as e:
                logger.error(f"[Telegram] stop Deribit 撤单失败: {e}")
                _stop_msgs.append(f"⚠️ Deribit 撤单出错: {e}")
            # 2. 撤销 Binance 所有挂单（WS 已连接用现有连接，否则 REST 降级）
            _bn_auth = getattr(self.engine, 'binance_auth', None)
            if self.engine.binance_ws and self.engine.binance_connected:
                try:
                    _perp_sym = self.engine.binance_matcher.perpetual_symbol if hasattr(self.engine, 'binance_matcher') else 'BTCUSDT'
                    await self.engine.binance_executor.ws_client._rest_request(
                        "DELETE", "/fapi/v1/allOpenOrders",
                        {"symbol": _perp_sym}, signed=True)
                    _stop_msgs.append("✅ Binance 挂单已撤销")
                except Exception as e:
                    logger.error(f"[Telegram] stop Binance 撤单失败: {e}")
                    _stop_msgs.append(f"⚠️ Binance 撤单出错: {e}")
            elif _bn_auth:
                try:
                    _perp_sym2 = self.engine.binance_matcher.perpetual_symbol if hasattr(self.engine, 'binance_matcher') else 'BTCUSDT'
                    async with aiohttp.ClientSession(
                        headers=_bn_auth.headers, timeout=aiohttp.ClientTimeout(total=10)) as _s:
                        _params = _bn_auth.sign({"symbol": _perp_sym2})
                        async with _s.delete(f"{_bn_auth.rest_base}/fapi/v1/allOpenOrders", params=_params) as _r:
                            await _r.json()
                    _stop_msgs.append("✅ Binance 挂单已撤销 (REST降级)")
                except Exception as e:
                    logger.error(f"[Telegram] stop Binance REST降级撤单失败: {e}")
                    _stop_msgs.append(f"⚠️ Binance 撤单出错: {e}")
            # 3. 附加盈利报告
            _profit_str = ""
            if hasattr(self.engine, '_start_balance_snap'):
                try:
                    _snap = await self.engine._snapshot_balances()
                    _profit_str = "\n" + self.engine._format_profit_report(self.engine._start_balance_snap, _snap)
                except Exception:
                    pass
            await self.send_async("🛑 系统已暂停交易\n" + "\n".join(_stop_msgs) + _profit_str + "\n扫描和下单均已挂起。")

        elif text_lower == 'start':
            # 维护期间允许手动触发一次独立探测；探测成功后继续走正常启动流程。
            if getattr(self.engine.client, 'maintenance_sleep_active', False):
                self.engine._remove_pause("手动stop")
                self.engine._manual_stop = False
                self.engine.trade_executor.emergency_stop = False
                _maintenance_recovered = await self.engine.client.probe_maintenance_recovery_once(notify=False)
                if not _maintenance_recovered:
                    await self.send_async(
                        "⚠️ Deribit 交易所正在维护中，系统维护保护仍然生效。\n"
                        "已清除手动暂停，维护结束后将自动恢复交易。")
                    return
                await self.send_async("✅ Deribit 维护探测成功，正在恢复系统连接与扫描。")

            # 保证金不足时，验证余额是否已充值再允许恢复
            _margin_block = False
            _margin_msgs = []
            for _mp in ("Binance保证金不足", "Deribit保证金不足"):
                if self.engine._has_pause(_mp):
                    try:
                        if "Binance" in _mp:
                            if not self.engine.binance_ws:
                                _margin_msgs.append("Binance WS 未连接，无法验证余额")
                                _margin_block = True
                                continue
                            _chk = await self.engine.binance_ws.get_account_info()
                            if not _chk:
                                _margin_msgs.append("Binance 余额查询失败，无法确认")
                                _margin_block = True
                                continue
                            _chk_avail = float(_chk.get('availableBalance', 0))
                            _bn_price = 0.0
                            if self.engine.binance_ws:
                                _bn_price = float(self.engine.binance_ws.mark_prices.get('BTCUSDT', 0))
                                if _bn_price <= 0:
                                    _bn_price = float(self.engine.binance_ws.last_prices.get('BTCUSDT', 0))
                            if _bn_price <= 0:
                                _bn_price = 75000.0
                            _chk_needed = float(self.engine.trade_amount) * _bn_price * 0.05 * 2.0
                            if _chk_avail < _chk_needed:
                                _margin_msgs.append(
                                    f"Binance 可用: {_chk_avail:.2f} USDT < 所需: {_chk_needed:.2f} USDT")
                                _margin_block = True
                                continue
                        if "Deribit" in _mp:
                            _dr = await self.engine.client.send_request({
                                "jsonrpc": "2.0",
                                "id": self.engine.client._get_next_request_id(),
                                "method": "private/get_account_summary",
                                "params": {"currency": self.engine.target_currency}
                            }, is_private=True)
                            if 'result' not in _dr:
                                _margin_msgs.append("Deribit 余额查询失败，无法确认")
                                _margin_block = True
                                continue
                            _dr_avail = float(_dr['result'].get('available_funds', 0))
                            if _dr_avail < 0.01:
                                _margin_msgs.append(
                                    f"Deribit 可用: {_dr_avail:.4f} {self.engine.target_currency}")
                                _margin_block = True
                                continue
                        self.engine._remove_pause(_mp)
                    except Exception as _me:
                        logger.warning(f"/start 保证金验证异常: {_me}")
                        _margin_block = True
            if _margin_block:
                _detail = "\n".join(f"  • {m}" for m in _margin_msgs) if _margin_msgs else "  • 验证过程异常，无法确认余额"
                await self.send_async(f"❌ 无法恢复: 保证金仍然不足\n{_detail}\n请先充值后再发送 /start")
                return

            self.engine.running = True
            self.engine._remove_pause("手动stop")
            self.engine._remove_pause("紧急清仓")
            self.engine._remove_pause("Binance对冲临时失败")
            # 🌟 2026-06-11: 持仓数据可疑暂停 = 等待人工核实, /start 即操作员确认
            # 确认后 15 分钟内空仓读数视同因果证据 (防"外部平仓无事件"场景死锁)
            # CR-3: 回看窗口覆盖"/start 处理中途暂停被竞态移除"场景;
            # R4-1: 暂停若已被数据恢复 auto-resolve (last_add <= auto_resolved), 惯性
            # /start 不再登记 ack — 数据已自证恢复时不应开 15 分钟放行窗口
            _last_add_ts = getattr(self.engine, '_bn_suspect_last_add_ts', 0.0)
            _auto_resolved_ts = getattr(self.engine, '_bn_suspect_auto_resolved_ts', 0.0)
            _suspect_recent = (
                self.engine._has_pause("Binance持仓数据可疑")
                or ((time.time() - _last_add_ts) < 120 and _last_add_ts > _auto_resolved_ts))
            if _suspect_recent:
                self.engine._bn_suspect_human_ack_ts = time.time()
                await self.send_async(
                    "⚠️ 已登记 Binance 持仓数据人工确认 — 15 分钟内系统将信任\"空仓\"读数, "
                    "可能据此平掉无对冲的期权腿。\n"
                    "请确保你已在 Binance 核实实仓确实为空; 若未核实, 请立即 /stop。")
            self.engine._remove_pause("Binance持仓数据可疑")
            self.engine._manual_stop = False
            self.engine.trade_executor.emergency_stop = False
            self.engine.trade_executor._stop_signal_logged = False
            if self.engine.trading_paused:
                await self.send_async(
                    "▶️ 已解除手动暂停。\n"
                    f"⚠️ 仍存在自动风控暂停原因: {self.engine._pause_reason}\n"
                    "待风险条件解除后系统会自动恢复开仓。")
            else:
                await self.send_async("▶️ 系统已恢复交易。")
        elif text_lower == 'status':
            _e = self.engine
            # 持仓统计 (先计算，后面判断需要用)
            active = sum(1 for s in _e.arbitrage_states.values() if s.state in ('position_open', 'executing', 'exiting'))
            max_pos = getattr(_e, 'max_total_positions', 10)
            # 系统运行状态
            if not _e.running:
                sys_state = "🔴 已停止"
            elif getattr(_e, 'trading_paused', False):
                reason = getattr(_e, '_pause_reason', '未知')
                sys_state = f"⏸️ 暂停中 (原因: {reason})"
            else:
                sys_state = "🟢 正常运行"
            # 开仓状态
            if not _e.running:
                open_state = "🔴 已停止"
            elif getattr(_e, 'trading_paused', False):
                open_state = f"⏸️ 暂停 ({getattr(_e, '_pause_reason', '未知')})"
            elif active >= max_pos:
                open_state = f"⏸️ 持仓已满 ({active}/{max_pos})"
            else:
                open_state = "🟢 正常"
            # 平仓状态 (monitor_positions 独立运行，不受 trading_paused 影响)
            if not _e.running:
                close_state = "🔴 已停止"
            elif not getattr(_e, 'initialized', False) or not _e.client.is_connected:
                close_state = "⚠️ WS 未连接，暂停监控"
            else:
                close_state = "🟢 正常"
            # Binance 连接
            bn_state = "🟢 已连接" if getattr(_e, 'binance_connected', False) else "🔴 未连接"
            # Deribit 连接
            dr_state = "🟢 已连接" if _e.client.is_connected else "🔴 未连接"

            _tier_block = ""
            _tex = getattr(_e, 'trade_executor', None)
            if _tex and hasattr(_tex, 'tier_stats') and _tex.tier_stats.get('total', 0) > 0:
                _s = _tex.tier_stats
                _fills = _s['T1_fill'] + _s['T2_fill'] + _s['T3_fill']
                _cancels = _s['cancel_profit'] + _s['cancel_timeout']
                _tot = _s['total']
                _fill_pct = f"{_fills/_tot*100:.0f}%" if _tot else "N/A"
                _maker_pct = f"{(_s['T1_fill']+_s['T2_fill'])/_tot*100:.0f}%" if _tot else "N/A"
                _tier_block = (
                    f"\n{'='*28}\n"
                    f"📈 三档递进统计\n"
                    f"T1(中间价): {_s['T1_fill']} | T2(插队): {_s['T2_fill']} | T3(对手): {_s['T3_fill']}\n"
                    f"取消: 利润↓={_s['cancel_profit']} 超时={_s['cancel_timeout']}\n"
                    f"回滚验证: {_s['rollback_verify']}\n"
                    f"成交率: {_fill_pct} | Maker率: {_maker_pct}"
                )
            await self.send_async(
                f"📊 系统状态\n"
                f"{'='*28}\n"
                f"系统: {sys_state}\n"
                f"开仓: {open_state}\n"
                f"平仓: {close_state}\n"
                f"{'='*28}\n"
                f"活跃组合: {active}/{max_pos}\n"
                f"Deribit: {dr_state}\n"
                f"Binance: {bn_state}\n"
                f"币种: {_e.target_currency}"
                f"{_tier_block}"
            )

        elif text_lower == 'pos':
            try:
                await self.send_async(f"⏳ 正在向交易所请求最新的 {self.engine.target_currency} 持仓数据...")
                # 强制发起 API 请求以获取绝对实时的标记价格和盈亏
                positions = await self.engine.client.get_positions(self.engine.target_currency)

                if not positions:
                    await self.send_async("📊 账户当前没有任何持仓。")
                    return

                # 计算总盈亏 (Deribit API 返回的 total_profit_loss 已被我们映射到 unrealized_pnl)
                total_upl = sum(p.unrealized_pnl for p in positions)

                # ================= Deribit 币本位盈亏逐仓折算 USD =================
                deribit_ref_price = Decimal('0')
                for p in positions:
                    if len(p.instrument_name.split('-')) <= 2 and p.mark_price > 0:
                        deribit_ref_price = p.mark_price
                        break
                if deribit_ref_price <= 0 and self.engine.binance_ws:
                    try:
                        _perp = self.engine.binance_matcher.perpetual_symbol if hasattr(self.engine, 'binance_matcher') else 'BTCUSDT'
                        _ob = self.engine.binance_ws.order_books.get(_perp)
                        if _ob and _ob.mid_price and _ob.mid_price > 0:
                            deribit_ref_price = Decimal(str(_ob.mid_price))
                    except Exception:
                        pass

                deribit_upl_usd = Decimal('0')
                for p in positions:
                    # 兼容字段差异：部分 Position 对象无 index_price 字段
                    _idx_px = Decimal(str(getattr(p, 'index_price', 0)))
                    _mark_px = Decimal(str(getattr(p, 'mark_price', 0)))
                    _px = _idx_px if _idx_px > 0 else (_mark_px if _mark_px > 0 else deribit_ref_price)
                    if _px > 0:
                        deribit_upl_usd += p.unrealized_pnl * _px

                # ===== 预先汇总 Binance 浮盈（USDT线性）并构建明细 =====
                bn_total_upnl = Decimal('0')
                bn_lines = []
                _bn_has_pos = False
                if self.engine.binance_ws:
                    if getattr(self.engine, 'binance_dual_side_mode', False) and getattr(self.engine.binance_ws, 'positions_by_side', None):
                        for (_bn_sym, _bn_ps), _bn_pos in self.engine.binance_ws.positions_by_side.items():
                            if _bn_pos.quantity <= 0:
                                continue
                            _bn_has_pos = True
                            _dir = str(_bn_ps).upper() if str(_bn_ps).upper() in ("LONG", "SHORT") else _bn_pos.side
                            _bn_dir = "🟢 多" if _dir == "LONG" else "🔴 空"
                            _bn_upnl = _bn_pos.unrealized_pnl if hasattr(_bn_pos, 'unrealized_pnl') else Decimal('0')
                            bn_total_upnl += Decimal(str(_bn_upnl))
                            bn_lines.append(
                                f"{_bn_dir} | {_bn_sym} ({_dir})\n"
                                f"📦 数量: {_bn_pos.quantity} | 入场: {_bn_pos.entry_price:.2f}\n"
                                f"💵 浮盈: {_bn_upnl:.2f} USDT\n"
                            )
                    else:
                        for _bn_sym, _bn_pos in self.engine.binance_ws.positions.items():
                            if _bn_pos.quantity <= 0:
                                continue
                            _bn_has_pos = True
                            _bn_dir = "🟢 多" if _bn_pos.side == "LONG" else "🔴 空"
                            _bn_upnl = _bn_pos.unrealized_pnl if hasattr(_bn_pos, 'unrealized_pnl') else Decimal('0')
                            bn_total_upnl += Decimal(str(_bn_upnl))
                            bn_lines.append(
                                f"{_bn_dir} | {_bn_sym}\n"
                                f"📦 数量: {_bn_pos.quantity} | 入场: {_bn_pos.entry_price:.2f}\n"
                                f"💵 浮盈: {_bn_upnl:.2f} USDT\n"
                            )
                total_upl_usd = deribit_upl_usd + bn_total_upnl

                msg_lines = [
                    f"📊 【实时持仓报告】 ({self.engine.target_currency})",
                    f"💰 Deribit未实现盈亏(币本位): {total_upl:.6f} {self.engine.target_currency}",
                    f"💵 组合未实现盈亏(折算USD): {total_upl_usd:.2f} "
                    f"(Deribit≈{deribit_upl_usd:.2f} + Binance={bn_total_upnl:.2f})",
                    "------------------------"
                ]

                # 整理具体持仓明细
                for p in positions:
                    # 简化合约名称让手机屏幕排版更好看 (例如抹去 BTC- 前缀)
                    inst = p.instrument_name.replace(f"{self.engine.target_currency}-", "")

                    # 区分多空方向图标
                    direction = "🟢 多" if p.size > 0 else "🔴 空"

                    # 格式化单条持仓信息
                    line = (
                        f"{direction} | {inst}\n"
                        f"📦 数量: {p.size} | 盈亏: {p.unrealized_pnl:.5f}\n"
                        f"📍 均价: {p.average_price:.2f} | 标价: {p.mark_price:.2f}\n"
                    )
                    msg_lines.append(line)

                # ===== Binance 持仓 =====
                if self.engine.binance_ws:
                    if _bn_has_pos:
                        msg_lines.append("--- Binance 期货 ---")
                        msg_lines.extend(bn_lines)
                    if not _bn_has_pos:
                        msg_lines.append("--- Binance: 无持仓 ---")

                # 附加引擎监控状态
                active_combos = len([s for s in self.engine.arbitrage_states.values() if s.state == 'position_open'])
                msg_lines.append("------------------------")
                msg_lines.append(f"🤖 引擎底层正在死锁监控 {active_combos} 个套利组合。")

                final_msg = "\n".join(msg_lines)

                # Telegram 消息长度限制保护 (超过 4000 字符分段或截断)
                if len(final_msg) > 4000:
                    await self.send_async(final_msg[:4000] + "\n...[由于字数限制已截断]")
                else:
                    await self.send_async(final_msg)

            except Exception as e:
                await self.send_async(f"❌ 获取实时持仓失败:\n报错: {e}")

        elif text_lower == 'config':
            try:
                # 1. 安全提取深层属性
                # 费率等级实际存储在费率计算器中
                current_tier = getattr(self.engine.fee_calculator, 'tier', 'standard')
                # 测试网状态可以通过 WebSocket 的 URL 来反向判断
                is_testnet = 'test' in self.engine.client.ws_url
                # 2. 组装消息 (全部使用 getattr 防呆，即使以后漏传了某个参数也不会崩溃)
                _e = self.engine
                _currency = getattr(_e, 'target_currency', 'BTC')
                _cfg_name = f"{_currency}_CONFIG"
                conf_str = (
                    f"===== 当前运行配置 =====\n"
                    f"币种: {_currency} | "
                    f"{'测试网' if is_testnet else '主网'} | "
                    f"费率: {current_tier}\n\n"
                    f"--- BASE_CONFIG ---\n"
                    f"scan_interval_ms: {getattr(_e, 'scan_interval_ms', 'N/A')}\n"
                    f"max_wait_time: {getattr(_e, 'max_wait_time', 60)}\n"
                    f"futures_numbers: {getattr(_e, 'futures_numbers', 'N/A')}\n"
                    f"current_tier: {current_tier}\n"
                    f"concurrent_batch_size: {getattr(_e, 'concurrent_batch_size', 'N/A')}\n"
                    f"batch_interval: {getattr(_e, 'batch_interval', 0.5)}\n"
                    f"global_max_delta: {getattr(_e, 'global_max_delta', 'N/A')}\n"
                    f"global_hard_delta: {getattr(_e, 'global_hard_delta', 'N/A')}\n"
                    f"max_total_positions: {getattr(_e, 'max_total_positions', 'N/A')}\n"
                    f"settlement_hard_stop_guard: {getattr(_e, 'settlement_hard_stop_guard', True)}\n"
                    f"settlement_hard_stop_grace_seconds: {getattr(_e, 'settlement_hard_stop_grace_seconds', 1200.0)}\n"
                    f"risk_alert_throttle_seconds: {getattr(_e, 'risk_alert_throttle_seconds', 300)}\n"
                    f"maker_top5_log_interval_seconds: {getattr(_e, 'maker_top5_log_interval_seconds', 300)}\n"
                    f"record_spread_snapshots: {getattr(_e, 'record_spread_snapshots', True)}\n\n"
                    f"--- {_cfg_name} ---\n"
                    f"min_profit_threshold: {getattr(_e, 'min_profit_threshold', 'N/A')}\n"
                    f"max_option_dte_hours: {getattr(_e, 'max_option_dte_hours', 72)}\n"
                    f"min_option_dte_hours: {getattr(_e, 'min_option_dte_hours', 12)}\n"
                    f"max_positions_per_expiry: {getattr(_e, 'max_positions_per_expiry', 3)}\n"
                    f"trade_amount: {getattr(_e, 'trade_amount', 'N/A')}\n"
                    f"moneyness_threshold: {getattr(_e, 'moneyness_threshold', 'N/A')}\n"
                    f"max_spread_pct: {getattr(_e, 'max_spread_pct', 'N/A')}\n"
                    f"min_option_volume: {getattr(_e, 'min_option_volume', 0)}\n"
                    f"maker_price_aggression: {getattr(_e, 'maker_price_aggression', 0.8)}\n"
                    f"max_net_gamma: {getattr(_e, 'max_net_gamma', Decimal('0.02'))}\n"
                    f"hard_stop_loss_usd: {getattr(_e, 'hard_stop_loss_usd', 'N/A')}\n"
                    f"daily_loss_limit_usd: {getattr(_e, 'daily_loss_limit_usd', 0)}\n"
                    f"daily_loss_auto_close: {getattr(_e, 'daily_loss_auto_close', False)}\n"
                    f"max_perpetual_hold_hours: {getattr(_e, 'max_perpetual_hold_hours', 80)}\n"
                    f"max_funding_rate_pct: {getattr(_e, 'max_funding_rate_pct', 0.001)}\n"
                    f"min_depth_ratio: {getattr(_e, 'min_depth_ratio', 0.2)}\n"
                    f"post_anchor_min_profit_usd: {getattr(_e, 'post_anchor_min_profit_usd', Decimal('12'))}\n"
                    f"rollback_ioc_aggressive_ticks: {getattr(_e, 'rollback_ioc_aggressive_ticks', 100)}\n"
                    f"post_fill_negative_action: {getattr(_e, 'post_fill_negative_action', 'hold')}\n\n"
                    f"--- BINANCE_CONFIG ---\n"
                    f"use_hedge_mode: {getattr(_e, 'binance_use_hedge_mode', True)}\n"
                    f"strict_hedge_mode: {getattr(_e, 'binance_strict_hedge_mode', False)}\n"
                    f"fee_tier: {getattr(_e.binance_fee_calc, 'tier', 'N/A') if getattr(_e, 'binance_fee_calc', None) else 'N/A'}\n"
                    f"binance_max_slippage_usd: {getattr(_e, 'binance_max_slippage_usd', 5.0)}\n"
                    f"binance_close_twap_slices: {getattr(_e, 'binance_close_twap_slices', 4)}\n"
                    f"binance_close_twap_interval_sec: {getattr(_e, 'binance_close_twap_interval_sec', 0.25)}\n"
                    f"settlement_twap_enabled: {getattr(_e, 'settlement_twap_enabled', True)}\n"
                    f"settlement_twap_minutes: {getattr(_e, 'settlement_twap_minutes', 30)}\n"
                    f"settlement_twap_slices: {getattr(_e, 'settlement_twap_slices', 30)}\n"
                    f"basis_monitor_hours: {getattr(_e, 'basis_monitor_hours', 3.0)}\n"
                    f"basis_early_trigger_usd: {getattr(_e, 'basis_early_trigger_usd', 300.0)}\n"
                    f"basis_deterioration_trigger_usd: {getattr(_e, 'basis_deterioration_trigger_usd', 150.0)}\n"
                    f"leverage: {config.BINANCE_CONFIG.get('leverage', 3)} (仅启动生效)\n"
                    f"margin_type: {config.BINANCE_CONFIG.get('margin_type', 'ISOLATED')} (仅启动生效)\n"
                    f"use_testnet: {config.BINANCE_CONFIG.get('use_testnet', True)} (仅启动生效)\n\n"
                    f"--- 不可热更新 (需重启) ---\n"
                    f"target_currency / test_trading / leverage / margin_type\n"
                    f"use_testnet / fee_tier / API_KEY\n\n"
                    f"热更新示例 (直接输入，无需斜杠):\n"
                    f"{_cfg_name}: min_option_volume = 10\n"
                    f"BASE_CONFIG: max_wait_time = 120\n"
                    f"BASE_CONFIG: daily_loss_limit_usd = 3000\n"
                    f"BINANCE_CONFIG: binance_max_slippage_usd = 8\n"
                    f"BINANCE_CONFIG: binance_close_twap_slices = 3"
                )

                await self.send_async(conf_str)

            except Exception as e:

                # 加上这层保护，即使发生错误也会把报错信息发到 Telegram，而不是直接装死

                await self.send_async(f"❌ 读取配置失败，报错信息: {e}")

        elif text_lower == 'stop_all':
            # 1. 立即停止引擎扫描，防止产生新订单
            self.engine._add_pause("紧急清仓")
            self.engine.running = False
            await self.send_async("🚨 【紧急清仓指令已启动】\n系统已暂停扫描。正在等待执行中任务退出并清仓...")

            # 2. 调用引擎执行清仓
            try:
                # 异步调用清仓逻辑
                results = await self.engine.emergency_liquidate_all()

                msg = "✅ 【清仓执行完毕】\n"
                for res in results:
                    msg += f"• {res}\n"
                # 附加盈利报告
                if hasattr(self.engine, '_start_balance_snap'):
                    try:
                        _snap = await self.engine._snapshot_balances()
                        _rpt = self.engine._format_profit_report(self.engine._start_balance_snap, _snap)
                        msg += f"\n{_rpt}"
                    except Exception:
                        pass
                msg += "\n系统已停止。发送 start 恢复扫描。"
                await self.send_async(msg)
            except Exception as e:
                await self.send_async(f"❌ 清仓过程中发生异常: {e}")

        elif text_lower == 'balancechg':
            try:
                if not hasattr(self.engine, '_start_balance_snap'):
                    await self.send_async("⚠️ 启动余额快照不可用，可能引擎尚未完全初始化。")
                    return
                await self.send_async("⏳ 正在查询余额变化...")
                current_snap = await self.engine._snapshot_balances()
                report = self.engine._format_balance_change(self.engine._start_balance_snap, current_snap)
                await self.send_async(report)
            except Exception as e:
                await self.send_async(f"❌ 余额变化查询失败: {e}")

        elif text_lower == 'balance':
            try:
                await self.send_async("⏳ 正在查询账户余额...")
                # --- Deribit ---
                try:
                    resp = await self.engine.client.send_request({
                        "jsonrpc": "2.0", "id": self.engine.client._get_next_request_id(),
                        "method": "private/get_account_summary",
                        "params": {"currency": self.engine.target_currency, "extended": True}
                    }, is_private=True)
                    if 'result' in resp:
                        r = resp['result']
                        cur = self.engine.target_currency
                        d_lines = [
                            f"--- Deribit ({cur}) ---",
                            f"权益(equity): {Decimal(str(r.get('equity', 0))):.6f}",
                            f"余额(balance): {Decimal(str(r.get('balance', 0))):.6f}",
                            f"可用资金: {Decimal(str(r.get('available_funds', 0))):.6f}",
                            f"初始保证金: {Decimal(str(r.get('initial_margin', 0))):.6f}",
                            f"维持保证金: {Decimal(str(r.get('maintenance_margin', 0))):.6f}",
                            f"未实现盈亏: {Decimal(str(r.get('futures_session_upl', 0))):.6f}",
                        ]
                    else:
                        d_lines = ["--- Deribit ---", "⚠️ 查询失败"]
                except Exception as e:
                    d_lines = ["--- Deribit ---", f"⚠️ 查询失败: {e}"]

                # --- Binance ---
                acct = None
                if self.engine.binance_ws and self.engine.binance_connected:
                    acct = await self.engine.binance_ws.get_account_info()
                elif getattr(self.engine, 'binance_auth', None):
                    async with aiohttp.ClientSession(
                        headers=self.engine.binance_auth.headers,
                        timeout=aiohttp.ClientTimeout(total=10)) as s:
                        params = self.engine.binance_auth.sign({})
                        async with s.get(
                            f"{self.engine.binance_auth.rest_base}/fapi/v2/account",
                            params=params) as _r:
                            acct = await _r.json()

                if acct and 'totalWalletBalance' in acct:
                    b_lines = [
                        f"--- Binance (USDT) ---",
                        f"权益(equity): {Decimal(str(acct.get('totalMarginBalance', 0))):.2f}",
                        f"余额(balance): {Decimal(str(acct.get('totalWalletBalance', 0))):.2f}",
                        f"可用资金: {Decimal(str(acct.get('availableBalance', 0))):.2f}",
                        f"未实现盈亏: {Decimal(str(acct.get('totalUnrealizedProfit', 0))):.2f}",
                    ]
                else:
                    b_lines = ["--- Binance (USDT) ---", "⚠️ 未连接或查询失败"]

                msg = "💰 账户余额\n" + "="*30 + "\n" + "\n".join(d_lines) + "\n\n" + "\n".join(b_lines)
                await self.send_async(msg)
            except Exception as e:
                await self.send_async(f"❌ 余额查询失败: {e}")

        elif text_lower == 'help':
            help_msg = (
                "系统可用指令清单:\n\n"
                "/status — 查看系统运行状态\n"
                "/pos — 查看当前实时持仓与盈亏\n"
                "/balance — 查看两个交易所的账户余额\n"
                "/balancechg — 查看启动以来的余额变化(已实现)\n"
                "/config — 查看系统运行参数\n"
                "/t — 获取系统最新的 15 行运行日志\n"
                "/stop — 紧急暂停交易 (撤单+盈利报告)\n"
                "/start — 恢复系统交易\n"
                "/stop_all — 紧急平掉所有持仓并暂停\n"
                "/help — 调出本条帮助信息\n\n"
                "热更新配置 (无需斜杠):\n"
                "  BTC_CONFIG: min_profit_threshold = 5\n"
                "  BASE_CONFIG: max_wait_time = 120"
            )
            await self.send_async(help_msg)

        elif ":" in text and "=" in text:
            # 解析动态调参: 如 "BTC_CONFIG: min_profit_threshold = 21" 或 "BASE_CONFIG: max_wait_time = 30"
            try:
                config_name, param_part = text.split(":", 1)
                config_name = config_name.strip()
                key, val = param_part.split("=", 1)
                key = key.strip()
                val = val.strip()
                # 字符串参数支持不带引号输入，自动补齐，避免写坏 config.py
                if key in {'current_tier', 'post_fill_negative_action', 'target_currency'}:
                    _v = val.strip()
                    if not ((_v.startswith("'") and _v.endswith("'")) or (_v.startswith('"') and _v.endswith('"'))):
                        val = f'"{_v}"'
                # 布尔参数规范化为 Python 字面量 True/False，避免写坏 config.py
                if key in {'settlement_hard_stop_guard', 'record_spread_snapshots'}:
                    _vb = val.strip().strip('"\'').lower()
                    if _vb in ('true', '1', 'yes', 'on'):
                        val = 'True'
                    elif _vb in ('false', '0', 'no', 'off'):
                        val = 'False'
                    else:
                        await self.send_async(f"❌ 解析失败: {key} 仅允许 true/false")
                        return
                if key == 'current_tier':
                    _tier = val.strip().strip('"\'').lower()
                    _tiers = set(getattr(self.engine.fee_calculator, 'fee_structure', {}).keys())
                    if not _tiers:
                        _tiers = {'standard', 'vip1'}
                    if _tier not in _tiers:
                        await self.send_async(
                            f"❌ 安全拦截: current_tier={_tier} 非法，允许值: {', '.join(sorted(_tiers))}")
                        return
                # 1. 逐行读取并修改物理 config.py 文件
                with open('config.py', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                in_target_dict = False
                modified = False
                for i, line in enumerate(lines):
                    # 判断是否进入了目标字典块
                    if re.match(rf"^{config_name}\s*=\s*{{", line):
                        in_target_dict = True
                        continue
                    if in_target_dict:
                        if line.strip() == "}":
                            break
                        # 正则匹配键值对 (适配带逗号和不带逗号的行末)
                        pattern = rf'([\'"]{key}[\'"]\s*:\s*)[^,\r\n]+'
                        if re.search(pattern, line):
                            lines[i] = re.sub(pattern, rf'\g<1>{val}', line)
                            modified = True
                            break
                if modified:
                    with open('config.py', 'w', encoding='utf-8') as f:
                        f.writelines(lines)
                else:
                    await self.send_async(f"❌ 修改失败：在 `{config_name}` 块中找不到键 `{key}`")
                    return
                # 2. 动态修改内存中的运行变量 (精准类型推断)
                current_config_name = "BTC_CONFIG" if getattr(self.engine, 'target_currency',
                                                              'BTC') == "BTC" else "ETH_CONFIG"
                # 无论是 BASE_CONFIG / 当前币种 Config / BINANCE_CONFIG，都执行热重载
                if config_name == current_config_name or config_name == "BASE_CONFIG" or config_name == "BINANCE_CONFIG":
                    val_clean = val.strip('"\'')  # 去除可能带的引号
                    # 强类型匹配：转换为 Decimal
                    if key in ['min_profit_threshold',
                               'post_anchor_min_profit_usd',
                               'trade_amount',
                               'moneyness_threshold', 'max_spread_pct',
                               'global_max_delta', 'global_hard_delta',
                               'hard_stop_loss_usd', 'max_net_gamma']:
                        # 🌟 已移除: 'global_hard_stop_loss' (见 __init__ 注释)
                        new_val = Decimal(str(val_clean))
                        # 关键风控参数安全边界校验
                        _safe_bounds = {
                            'global_max_delta': (Decimal('0.05'), Decimal('1.0')),
                            'global_hard_delta': (Decimal('0.20'), Decimal('2.0')),
                            'min_profit_threshold': (Decimal('0'), Decimal('500')),
                            'post_anchor_min_profit_usd': (Decimal('0'), Decimal('500')),
                            'trade_amount': (Decimal('0.01'), Decimal('100')),
                            'hard_stop_loss_usd': (Decimal('50'), Decimal('1000')),
                            'max_net_gamma': (Decimal('0.001'), Decimal('1.0')),
                        }
                        if key in _safe_bounds:
                            lo, hi = _safe_bounds[key]
                            if new_val < lo or new_val > hi:
                                await self.send_async(f"❌ 安全拦截: {key}={new_val} 超出允许范围 [{lo}, {hi}]")
                                return
                        setattr(self.engine, key, new_val)
                    # 强类型匹配：转换为 int
                    elif key in ['min_option_volume', 'maker_price_aggression']:
                        new_val = float(val_clean)
                        _float_bounds = {
                            'min_option_volume': (0, 10000),
                            'maker_price_aggression': (0.1, 1.0),
                        }
                        lo, hi = _float_bounds[key]
                        if new_val < lo or new_val > hi:
                            await self.send_async(f"❌ 安全拦截: {key}={new_val} 超出允许范围 [{lo}, {hi}]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key in ['futures_numbers', 'max_wait_time', 'concurrent_batch_size', 'scan_interval_ms',
                                 'max_total_positions', 'max_positions_per_expiry',
                                 'max_perpetual_hold_hours', 'rollback_ioc_aggressive_ticks',
                                 'maker_top5_log_interval_seconds']:
                        _int_val = int(val_clean)
                        if key == 'maker_top5_log_interval_seconds':
                            if _int_val < 1 or _int_val > 3600:
                                await self.send_async(
                                    "❌ 安全拦截: maker_top5_log_interval_seconds 允许范围 [1, 3600] 秒")
                                return
                        elif key == 'max_perpetual_hold_hours':
                            if _int_val < 1 or _int_val > 240:
                                await self.send_async(
                                    "❌ 安全拦截: max_perpetual_hold_hours 允许范围 [1, 240] 小时")
                                return
                        elif key == 'rollback_ioc_aggressive_ticks':
                            if _int_val < 1 or _int_val > 2000:
                                await self.send_async(
                                    "❌ 安全拦截: rollback_ioc_aggressive_ticks 允许范围 [1, 2000]")
                                return
                        setattr(self.engine, key, _int_val)
                    elif key == 'min_option_dte_hours':
                        new_val = int(val_clean)
                        if new_val < 0 or new_val > 72:
                            await self.send_async("❌ 安全拦截: min_option_dte_hours 允许范围 [0, 72]")
                            return
                        if new_val >= int(getattr(self.engine, 'max_option_dte_hours', 72)):
                            await self.send_async(
                                f"❌ 安全拦截: min_option_dte_hours({new_val}) 必须小于 max_option_dte_hours({self.engine.max_option_dte_hours})")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'binance_close_twap_slices':
                        new_val = int(val_clean)
                        if new_val < 1 or new_val > 20:
                            await self.send_async("❌ 安全拦截: binance_close_twap_slices 允许范围 [1, 20]")
                            return
                        setattr(self.engine, key, new_val)
                    # 🌟 BINANCE_CONFIG 热更新: Binance 侧参数分支
                    elif key == 'binance_max_slippage_usd':
                        new_val = Decimal(str(val_clean))
                        if new_val < Decimal('1') or new_val > Decimal('100'):
                            await self.send_async("❌ 安全拦截: binance_max_slippage_usd 允许范围 [1, 100] USD")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'binance_close_twap_interval_sec':
                        new_val = float(val_clean)
                        if new_val < 0.05 or new_val > 5.0:
                            await self.send_async("❌ 安全拦截: binance_close_twap_interval_sec 允许范围 [0.05, 5.0] 秒")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_enabled':
                        new_val = val_clean.lower() in ('true', '1', 'yes', 'on')
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_minutes':
                        new_val = int(val_clean)
                        if new_val < 5 or new_val > 60:
                            await self.send_async("❌ 安全拦截: settlement_twap_minutes 允许范围 [5, 60]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_slices':
                        new_val = int(val_clean)
                        if new_val < 2 or new_val > 30:
                            await self.send_async("❌ 安全拦截: settlement_twap_slices 允许范围 [2, 30]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_monitor_hours':
                        new_val = float(val_clean)
                        if new_val < 0.5 or new_val > 12:
                            await self.send_async("❌ 安全拦截: basis_monitor_hours 允许范围 [0.5, 12]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_early_trigger_usd':
                        new_val = float(val_clean)
                        if new_val < 50 or new_val > 2000:
                            await self.send_async("❌ 安全拦截: basis_early_trigger_usd 允许范围 [50, 2000]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_deterioration_trigger_usd':
                        new_val = float(val_clean)
                        if new_val < 30 or new_val > 1000:
                            await self.send_async("❌ 安全拦截: basis_deterioration_trigger_usd 允许范围 [30, 1000]")
                            return
                        setattr(self.engine, key, new_val)
                    # 特殊对象匹配：费率等级 (需要穿透更新费率计算器)
                    elif key == 'current_tier':
                        _tier = val_clean.lower()
                        _fee_structure = getattr(self.engine.fee_calculator, 'fee_structure', {})
                        if _tier not in _fee_structure:
                            await self.send_async(
                                f"❌ 安全拦截: current_tier={_tier} 非法，允许值: {', '.join(sorted(_fee_structure.keys()))}")
                            return
                        self.engine.fee_calculator.tier = _tier
                        self.engine.fee_calculator.current_rates = _fee_structure[_tier]
                    elif key == 'post_fill_negative_action':
                        _action = val_clean.lower()
                        if _action not in ('hold', 'rollback'):
                            await self.send_async("❌ 安全拦截: post_fill_negative_action 仅允许 hold / rollback")
                            return
                        setattr(self.engine, key, _action)
                    elif key == 'min_depth_ratio':
                        new_val = float(val_clean)
                        if new_val < 0.05 or new_val > 1.0:
                            await self.send_async("❌ 安全拦截: min_depth_ratio 允许范围 [0.05, 1.0]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'daily_loss_limit_usd':
                        new_val = float(val_clean)
                        if new_val < 0 or new_val > 100000:
                            await self.send_async("❌ 安全拦截: daily_loss_limit_usd 允许范围 [0, 100000] USD (0=禁用)")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'max_funding_rate_pct':
                        new_val = float(val_clean)
                        if new_val < 0 or new_val > 0.01:
                            await self.send_async("❌ 安全拦截: max_funding_rate_pct 允许范围 [0, 0.01] (1%)")
                            return
                        setattr(self.engine, key, new_val)
                    elif key in ('use_hedge_mode', 'strict_hedge_mode', 'daily_loss_auto_close', 'record_spread_snapshots'):
                        _vb = val_clean.lower()
                        # 这些布尔参数在 engine 上的实际属性名:
                        _attr_map = {
                            'use_hedge_mode': 'binance_use_hedge_mode',
                            'strict_hedge_mode': 'binance_strict_hedge_mode',
                            'daily_loss_auto_close': 'daily_loss_auto_close',
                            'record_spread_snapshots': 'record_spread_snapshots',
                        }
                        _engine_attr = _attr_map[key]
                        if _vb in ('true', '1', 'yes', 'on'):
                            setattr(self.engine, _engine_attr, True)
                        elif _vb in ('false', '0', 'no', 'off'):
                            setattr(self.engine, _engine_attr, False)
                        else:
                            await self.send_async(f"❌ 安全拦截: {key} 仅允许 true/false")
                            return
                    elif key == 'risk_alert_throttle_seconds':
                        new_val = float(val_clean)
                        if new_val < 30 or new_val > 3600:
                            await self.send_async("❌ 安全拦截: risk_alert_throttle_seconds 允许范围 [30, 3600] 秒")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_hard_stop_grace_seconds':
                        new_val = float(val_clean)
                        if new_val < 0 or new_val > 1800:
                            await self.send_async("❌ 安全拦截: settlement_hard_stop_grace_seconds 允许范围 [0, 1800] 秒")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_hard_stop_guard':
                        _v = val_clean.lower()
                        if _v in ('true', '1', 'yes', 'on'):
                            setattr(self.engine, key, True)
                        elif _v in ('false', '0', 'no', 'off'):
                            setattr(self.engine, key, False)
                        else:
                            await self.send_async("❌ 安全拦截: settlement_hard_stop_guard 仅允许 true/false")
                            return
                    elif key == 'max_option_dte_hours':
                        new_val = int(val_clean)
                        if new_val < 24 or new_val > 168:
                            await self.send_async(f"❌ 安全拦截: {key}={new_val} 超出允许范围 [24, 168]")
                            return
                        _min_dte = int(getattr(self.engine, 'min_option_dte_hours', 12))
                        if new_val <= _min_dte:
                            await self.send_async(
                                f"❌ 安全拦截: max_option_dte_hours({new_val}) 必须大于 min_option_dte_hours({_min_dte})")
                            return
                        setattr(self.engine, key, int(val_clean))
                    # 强类型匹配：转换为 float
                    elif key in ['batch_interval']:
                        setattr(self.engine, key, float(val_clean))
                    elif key == 'binance_close_twap_interval_sec':
                        new_val = float(val_clean)
                        if new_val < 0.05 or new_val > 2.0:
                            await self.send_async("❌ 安全拦截: binance_close_twap_interval_sec 允许范围 [0.05, 2.0] 秒")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_enabled':
                        new_val = val_clean.lower() in ('true', '1', 'yes', 'on')
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_minutes':
                        new_val = int(val_clean)
                        if new_val < 5 or new_val > 60:
                            await self.send_async("❌ 安全拦截: settlement_twap_minutes 允许范围 [5, 60]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'settlement_twap_slices':
                        new_val = int(val_clean)
                        if new_val < 2 or new_val > 30:
                            await self.send_async("❌ 安全拦截: settlement_twap_slices 允许范围 [2, 30]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_monitor_hours':
                        new_val = float(val_clean)
                        if new_val < 0.5 or new_val > 12:
                            await self.send_async("❌ 安全拦截: basis_monitor_hours 允许范围 [0.5, 12]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_early_trigger_usd':
                        new_val = float(val_clean)
                        if new_val < 50 or new_val > 2000:
                            await self.send_async("❌ 安全拦截: basis_early_trigger_usd 允许范围 [50, 2000]")
                            return
                        setattr(self.engine, key, new_val)
                    elif key == 'basis_deterioration_trigger_usd':
                        new_val = float(val_clean)
                        if new_val < 30 or new_val > 1000:
                            await self.send_async("❌ 安全拦截: basis_deterioration_trigger_usd 允许范围 [30, 1000]")
                            return
                        setattr(self.engine, key, new_val)
                    # 底层环境匹配：修改后必须重启
                    elif key in ['target_currency', 'test_trading']:
                        await self.send_async(
                            f"⚠️ 文件配置已更新\n{key} = {val}\n(注: 修改币种或测试网环境，需要重启程序底层 WebSocket 才能生效！)")
                        return
                    await self.send_async(f"✅ 配置已更新且内存已热重载\n{config_name} -> {key} = {val}")
                else:
                    # 改了另外一个币种的参数，只存文件，不污染当前内存
                    await self.send_async(
                        f"✅ 文件配置已更新\n(注: 当前运行的是 {self.engine.target_currency}，您修改的 {config_name} 将在下次切换生效)\n{config_name} -> {key} = {val}")
            except Exception as e:
                await self.send_async(f"❌ 解析或修改配置失败\n报错: {e}")
                traceback.print_exc()
        else:
            await self.send_async(f"无效命令！")


# 全局实例
tg_notifier = TelegramNotifier(
    config.TELEGRAM_CONFIG.get('TG_BOT_TOKEN', ''),
    config.TELEGRAM_CONFIG.get('TG_CHAT_ID', ''),
    config.TELEGRAM_CONFIG.get('TG_ALLOWED_USER_IDS', '')
)
