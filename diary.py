"""模型日记：本地 SQLite 存储、聊天流归属与时间处理。

- 写入：``write_diary`` 工具把模型想记住的内容存为一条日记（追加式，不覆盖），
  自动记录写入时的聊天流（``stream_id``）；
- 回看：记忆检索时作为第四路数据源（标注 ``[日记]`` 前缀，最新在前），
  可见范围跟随「跨聊天流检索」设置——关闭=当前流；仅群聊=当前流+全部群聊；
  全部=全流可见（与段落/事实路的隐私语义一致）；
- 存储：独立 SQLite（插件数据目录 ``data/plugins/<插件 ID>/diary.db``），
  与宿主 A_Memorix 记忆库完全分离，不触碰宿主任何数据。

时间处理约定（本模块的「时间」即指此）：
- ``created_at`` 统一存 Unix 时间戳（UTC 秒）——排序、比较无时区歧义；
- 一切对外展示经 :func:`format_local_time` 转为运行环境本地时间
  （``YYYY-MM-DD HH:MM``），模型看到的每条日记都带本地时间；
- 索引建在 ``created_at`` 上，按时间倒序取数零成本。

聊天流归属与迁移：
- ``stream_id`` 记录写入时的聊天流；空串表示无归属（数据异常或旧版本写入）；
- 旧库（0.1.x 无该列）在首次连接时自动 ``ALTER TABLE`` 补列，旧条目为空串；
- 检索过滤：跨流模式全收；本流模式 = 当前流 + 空归属（旧数据保持可见，孤儿不丢）。
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_SCHEMA_STATEMENTS: Tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS diary_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content TEXT NOT NULL,
        created_at REAL NOT NULL,
        stream_id TEXT NOT NULL DEFAULT ''
    )
    """,
)

_INDEX_STATEMENTS: Tuple[str, ...] = (
    """
    CREATE INDEX IF NOT EXISTS idx_diary_entries_created_at
    ON diary_entries (created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_diary_entries_stream
    ON diary_entries (stream_id)
    """,
)


def format_local_time(timestamp: float) -> str:
    """Unix 时间戳 → 运行环境本地时间文本（``YYYY-MM-DD HH:MM``）。"""
    try:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return "未知时间"


_WEEKDAY_NAMES: Tuple[str, ...] = (
    "星期一",
    "星期二",
    "星期三",
    "星期四",
    "星期五",
    "星期六",
    "星期日",
)


def format_chinese_date(timestamp: float) -> str:
    """Unix 时间戳 → 中文日期（``2026年9月22日 星期二``，本地时间）。"""
    try:
        local = datetime.fromtimestamp(float(timestamp))
    except (OverflowError, OSError, ValueError):
        return "未知日期"
    return f"{local.year}年{local.month}月{local.day}日 {_WEEKDAY_NAMES[local.weekday()]}"


def format_chinese_time(timestamp: float) -> str:
    """Unix 时间戳 → 口语化时间（``下午4点`` / ``下午4点35分``，本地时间）。"""
    try:
        local = datetime.fromtimestamp(float(timestamp))
    except (OverflowError, OSError, ValueError):
        return "未知时间"
    hour, minute = local.hour, local.minute
    if hour == 0:
        period, hour12 = "午夜", 12
    elif hour < 6:
        period, hour12 = "凌晨", hour
    elif hour < 12:
        period, hour12 = "早上", hour
    elif hour == 12:
        period, hour12 = "中午", 12
    elif hour < 18:
        period, hour12 = "下午", hour - 12
    else:
        period, hour12 = "晚上", hour - 12
    minute_part = f"{minute:02d}分" if minute else ""
    return f"{period}{hour12}点{minute_part}"


class DiaryStore:
    """日记库读写（每次操作短连接；库小、单进程使用，无需连接池）。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=1.5)
        connection.row_factory = sqlite3.Row
        if not self._initialized:
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            self._migrate_stream_column(connection)
            for statement in _INDEX_STATEMENTS:
                connection.execute(statement)
            connection.commit()
            self._initialized = True
        return connection

    @staticmethod
    def _migrate_stream_column(connection: sqlite3.Connection) -> None:
        """旧库升级：补聊天流归属列（旧条目为空串 = 无归属，检索时保持可见）。"""
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(diary_entries)")
        }
        if "stream_id" not in columns:
            connection.execute(
                "ALTER TABLE diary_entries ADD COLUMN stream_id TEXT NOT NULL DEFAULT ''"
            )

    def add(self, content: str, stream_id: str = "") -> Tuple[int, float]:
        """追加一条日记，返回 ``(entry_id, created_at)``。"""
        created_at = time.time()
        connection = self._connect()
        try:
            cursor = connection.execute(
                "INSERT INTO diary_entries (content, created_at, stream_id) "
                "VALUES (?, ?, ?)",
                (content, created_at, str(stream_id or "").strip()),
            )
            connection.commit()
            return int(cursor.lastrowid or 0), created_at
        finally:
            connection.close()

    def search(
        self,
        terms: List[str],
        limit: int,
        *,
        stream_ids: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """按关键词匹配（命中词数计分），命中多者优先、同分按时间倒序。

        范围过滤与其它数据源一致：``stream_ids=None`` 全流可见（「全部」）；
        否则仅给定流 + 空归属条目（旧数据保持可见，孤儿不丢）。
        """
        cleaned = [term.strip().lower() for term in terms if term.strip()]
        if not cleaned:
            return []
        connection = self._connect()
        try:
            if stream_ids is None:
                rows = connection.execute(
                    "SELECT id, content, created_at, stream_id FROM diary_entries"
                ).fetchall()
            else:
                ids = sorted(
                    {str(item).strip() for item in stream_ids if str(item).strip()}
                )
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    rows = connection.execute(
                        "SELECT id, content, created_at, stream_id FROM diary_entries "
                        f"WHERE stream_id IN ({placeholders}) OR stream_id = ''",
                        ids,
                    ).fetchall()
                else:
                    rows = connection.execute(
                        "SELECT id, content, created_at, stream_id FROM diary_entries "
                        "WHERE stream_id = ''"
                    ).fetchall()
        finally:
            connection.close()

        matched: List[Tuple[int, float, Dict[str, Any]]] = []
        for row in rows:
            haystack = str(row["content"] or "").lower()
            score = sum(1 for term in cleaned if term in haystack)
            if score <= 0:
                continue
            matched.append((score, float(row["created_at"] or 0.0), dict(row)))
        matched.sort(key=lambda item: (-item[0], -item[1]))
        return [item[2] for item in matched[: max(1, limit)]]

    def list_by_stream(self, stream_id: str, limit: int) -> List[Dict[str, Any]]:
        """取某聊天流（+空归属旧条目）的最近日记，时间正序返回（日记回看用）。

        与检索路同款可见语义：本流 + 空归属（旧数据保持可见，孤儿不丢）。
        取最新 ``limit`` 条后翻转为正序——先写的先展示，符合日记叙事。
        """
        clean_stream = str(stream_id or "").strip()
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT id, content, created_at, stream_id FROM diary_entries "
                "WHERE stream_id = ? OR stream_id = '' "
                "ORDER BY created_at DESC LIMIT ?",
                (clean_stream, max(1, int(limit))),
            ).fetchall()
            return [dict(row) for row in reversed(rows)]
        finally:
            connection.close()

    def list_all(self, limit: int) -> List[Dict[str, Any]]:
        """取全库最近日记（跨流），时间正序返回（删除管理用）。

        不同于 ``list_by_stream`` 只取本流（+空归属），本方法不做流过滤，
        供 /删日记 查看「所有日记」以便跨流删除管理。取最新 ``limit`` 条后
        翻转为正序。返回的行带 ``stream_id``，便于区分来源流。
        """
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT id, content, created_at, stream_id FROM diary_entries "
                "ORDER BY created_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            return [dict(row) for row in reversed(rows)]
        finally:
            connection.close()

    def get_by_id(self, entry_id: int) -> Optional[Dict[str, Any]]:
        """按编号取单条日记（删除前回显内容用）；不存在返回 None。"""
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT id, content, created_at, stream_id FROM diary_entries "
                "WHERE id = ?",
                (int(entry_id),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def delete_by_id(self, entry_id: int) -> bool:
        """按编号删除一条日记，返回是否存在并已删除。"""
        connection = self._connect()
        try:
            cursor = connection.execute(
                "DELETE FROM diary_entries WHERE id = ?", (int(entry_id),)
            )
            connection.commit()
            return cursor.rowcount > 0
        finally:
            connection.close()

    def count(self) -> int:
        """日记总条数（观测与排查用）。"""
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM diary_entries"
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            connection.close()
