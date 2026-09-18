"""记忆增强 — 跨聊天流、事实声明与日记的增强版长期记忆。

工具（planner）：
- ``search_memory``（默认，新增工具模式）：始终随工具列表提供，引导模型主动检索；
- ``query_memory``（可选，替换原生模式）：先在宿主关闭内置记忆检索后同名接管
  （planner 视角零变化）；未关闭内置时插件同名工具会被跳过（宿主日志出现
  “检测到重复工具名”告警，加载时本插件亦会提示）；
- ``write_diary``：模型日记——随时记录想记住的内容，检索时可回看。

两种检索工具的接入选一（配置「工具」页签）：默认新增 ``search_memory`` 并由
planner 钩子自动从列表移除内置 ``query_memory``；开启「替换原生」则改用
``query_memory`` 同名接管。工具可见性均以 ``visibility="deferred"`` 为基态，
由 planner 钩子按配置补回常驻定义（standards/host-behavior.md §11/§12 的既有手法）。

四路数据源（各自独立开关，全部关闭时工具返回失败提示）：
- 段落/关系路：宿主能力 knowledge.search（mode=search），A_Memorix 双路检索；
- 事实路：只读直读 A_Memorix 的 metadata.db（fact_claims 表）——状态过滤
  （active）、过期过滤（valid_to）、范围过滤（person 级全局 / chat 级按流）+ LIKE 匹配；
- 日记路：本插件自有的日记库（diary.py，SQLite）——模型写下的日记，标注 [日记]；
- 经历路（默认关，检索较重）：knowledge.search（mode=episode）。

检索范围由 [search].cross_scope 三态决定：关闭 = 当前聊天流（respect_filter=true，
尊重宿主的记忆过滤策略）；仅群聊 = 当前流 + 全部群聊（逐流检索后合并，不含其它
私聊）；全部 = 全库（chat_id 置空，宿主 scope=None 即不过滤，与宿主启发式记忆召回
的 heuristic_memory_cross_chat_enabled 行为一致）。「仅群聊」的群聊流列表经
chat.get_group_streams 枚举（session_id 与记忆归属 key 同源）。

除日记外只读：不写入任何宿主记忆数据，不新增 embedding 调用。
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple

from maibot_sdk import (
    CONFIG_RELOAD_SCOPE_SELF,
    Field,
    HookHandler,
    MaiBotPlugin,
    PluginConfigBase,
    Tool,
)
from maibot_sdk.types import (
    ErrorPolicy,
    HookMode,
    HookOrder,
    ToolParameterInfo,
    ToolParamType,
)

from .diary import DiaryStore, format_local_time

SUPPORTED_CONFIG_VERSION = "0.2.0"
"""插件支持的配置版本（plugin.config_version 默认值，宿主据此执行配置迁移）。"""

_CAPABILITY_PREFIX = "你知道这些知识:"
"""宿主 knowledge.search 能力返回文本的包装前缀（剥离后保留检索正文）。"""

_CAPABILITY_MISS_PREFIX = "你不太了解有关"
"""宿主知识能力在无结果/被记忆过滤拦截时返回的礼貌话术前缀（非真实记忆内容）。"""

_BUILTIN_TOOL_SWITCH = "a_memorix.integration.enable_memory_query_tool"
"""宿主内置 query_memory 的启用开关（同名接管的前置条件）。"""

_TERM_SPLIT_PATTERN = re.compile(r"[\s,，。;；:：、/|\\]+")
"""事实 LIKE 匹配的查询切分（空白与常见标点）。"""

_FACT_STATUS_ACTIVE = "active"
"""fact_claims 的有效状态（另有 conflicted / superseded / retracted）。"""

_HOST_THRESHOLD_KEYS: Dict[str, str] = {
    "min_results": "a_memorix.threshold.min_results",
    "percentile": "a_memorix.threshold.percentile",
    "min_threshold": "a_memorix.threshold.min_threshold",
    "max_threshold": "a_memorix.threshold.max_threshold",
}
"""宿主检索阈值配置路径（只读展示用；快照在加载与配置更新时刷新）。"""

_DIARY_DB_FILENAME = "diary.db"
"""日记库文件名（位于插件数据目录 data/plugins/<插件 ID>/）。"""

_DIARY_MAX_CHARS = 2000
"""单条日记的内容长度上限（超出截断并提示）。"""

_ALL_PLATFORMS = "all_platforms"
"""ctx.chat.get_group_streams 的平台参数：获取所有平台的聊天流（先例：reply-control）。"""

_GROUP_STREAM_CACHE_SECONDS = 120.0
"""群聊流列表缓存时长（秒）：「仅群聊」范围解析时避免每次检索都拉取流列表。"""

_MAX_SCOPE_ROUTES = 20
"""「仅群聊」模式的最大检索路数（当前流 + 群聊流）；超出部分截断并提示。"""


def _search_tool_parameters() -> List[ToolParameterInfo]:
    """两个检索工具（search_memory / query_memory）共用的参数定义。"""
    return [
        ToolParameterInfo(
            name="query",
            param_type=ToolParamType.STRING,
            description="单个主题、关键词或模糊主题短语；不要把多个主题拼在一起。",
            required=True,
        ),
        ToolParameterInfo(
            name="limit",
            param_type=ToolParamType.INTEGER,
            description="返回条数上限（默认 5，最大 20）。",
            required=False,
        ),
        ToolParameterInfo(
            name="include_fact_evidence",
            param_type=ToolParamType.BOOLEAN,
            description="事实命中是否附带证据条数；默认 false。",
            required=False,
        ),
    ]


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="配置版本号（宿主迁移用，勿手改）",
        json_schema_extra={"hidden": True},
    )


class SourcesSectionConfig(PluginConfigBase):
    """数据源开关（每路独立）。"""

    __ui_label__ = "数据源"
    __ui_icon__ = "database"
    __ui_order__ = 1

    diaries: bool = Field(
        default=True,
        description="模型日记（本插件 write_diary 写入的内容，检索时标注 [日记]）",
        json_schema_extra={
            "label": "日记",
            "hint": "模型写下的日记（本插件自有数据库）；关闭后检索不返回日记内容（记录功能不受影响）",
        },
    )
    paragraphs_relations: bool = Field(
        default=True,
        description="段落 + 图谱关系（经宿主 knowledge.search 能力检索）",
        json_schema_extra={
            "label": "段落与关系",
            "hint": "长期记忆的段落与图谱关系检索；关闭后本路不返回结果",
        },
    )
    facts: bool = Field(
        default=True,
        description="事实声明（只读直读 A_Memorix metadata.db 的 fact_claims 表）",
        json_schema_extra={
            "label": "事实声明",
            "hint": "只读直读记忆库的「事实」表（宿主内置检索不含它）；库不存在时本路自动跳过并提示一次",
        },
    )
    episodes: bool = Field(
        default=False,
        description="经历/事件（knowledge.search mode=episode，检索较重）",
        json_schema_extra={
            "label": "经历与事件",
            "hint": "经历/事件类记忆（聚合管线，检索较重）；需要时再开启",
        },
    )


class SearchSectionConfig(PluginConfigBase):
    """检索行为配置。"""

    __ui_label__ = "检索"
    __ui_icon__ = "search"
    __ui_order__ = 2

    cross_scope: Literal["关闭", "仅群聊", "全部"] = Field(
        default="关闭",
        description="跨聊天流检索范围（关闭 / 仅群聊 / 全部）",
        json_schema_extra={
            "label": "跨聊天流检索",
            "hint": "关闭（默认）= 仅当前聊天流（与内置 query_memory 一致）；仅群聊 = 当前流 + 全部群聊，不含其它私聊（保护私聊隐私；群多时逐群检索略慢）；全部 = 全部流（含其它私聊）——后两者属隐私敏感能力",
        },
    )


class ToolsSectionConfig(PluginConfigBase):
    """工具接入方式。"""

    __ui_label__ = "工具"
    __ui_icon__ = "wrench"
    __ui_order__ = 3

    replace_builtin: bool = Field(
        default=False,
        description="替换原生：以 query_memory 同名工具接管宿主内置版（需先关闭宿主内置记忆检索）",
        json_schema_extra={
            "label": "替换原生 query_memory",
            "hint": "关（默认）= 新增 search_memory 工具，并自动从 planner 列表移除内置 query_memory（装上即用）；开 = 用 query_memory 同名接管（planner 视角零变化）——需先在宿主 WebUI「记忆」页关闭「启用记忆检索」，否则同名工具不生效（加载时会有日志提示）",
        },
    )


class DiarySectionConfig(PluginConfigBase):
    """日记功能。"""

    __ui_label__ = "日记"
    __ui_icon__ = "book"
    __ui_order__ = 4

    enabled: bool = Field(
        default=True,
        description="启用 write_diary 工具（模型随时记录日记，检索时可回看）",
        json_schema_extra={
            "label": "日记工具",
            "hint": "给模型一个 write_diary 工具随时记录想记住的内容（长期保存，检索时以 [日记] 前缀回看）；关闭后工具不再提供，已记录内容保留",
        },
    )


class HostRetrievalSectionConfig(PluginConfigBase):
    """宿主检索参数（只读展示：本插件不读取这些值，仅显示当前设置与修改入口）。"""

    __ui_label__ = "宿主检索参数"
    __ui_icon__ = "sliders-horizontal"
    __ui_order__ = 5

    min_results: int = Field(
        default=4,
        description="宿主 A_Memorix「最小保留数」（只读展示）",
        json_schema_extra={
            "label": "最小保留数（只读）",
            "hint": "宿主检索不足该条数时会降标准补足（默认 4，这是「不相关也返回」的来源；建议调到 1~2）。修改：宿主 WebUI「记忆」页 → 高级 → 最小保留数",
            "disabled": True,
        },
    )
    percentile: int = Field(
        default=75,
        description="宿主 A_Memorix「动态百分位」（只读展示）",
        json_schema_extra={
            "label": "动态百分位（只读）",
            "hint": "宿主动态阈值百分位（默认 75；建议调到 45~55，越小越严格）。属进阶调整，修改方式见插件 README「进阶」小节",
            "disabled": True,
        },
    )
    min_threshold: float = Field(
        default=0.29,
        description="宿主 A_Memorix「最小阈值」（只读展示）",
        json_schema_extra={
            "label": "最小阈值（只读）",
            "hint": "宿主动态阈值的下限（默认 0.29；一般保持默认）。属进阶调整，修改方式见插件 README「进阶」小节",
            "disabled": True,
        },
    )
    max_threshold: float = Field(
        default=0.95,
        description="宿主 A_Memorix「最大阈值」（只读展示）",
        json_schema_extra={
            "label": "最大阈值（只读）",
            "hint": "宿主动态阈值的上限（默认 0.95；一般保持默认）。属进阶调整，修改方式见插件 README「进阶」小节",
            "disabled": True,
        },
    )


class MemoryEnhanceConfig(PluginConfigBase):
    """插件总配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    sources: SourcesSectionConfig = Field(default_factory=SourcesSectionConfig)
    search: SearchSectionConfig = Field(default_factory=SearchSectionConfig)
    tools: ToolsSectionConfig = Field(default_factory=ToolsSectionConfig)
    diary: DiarySectionConfig = Field(default_factory=DiarySectionConfig)
    host_retrieval: HostRetrievalSectionConfig = Field(
        default_factory=HostRetrievalSectionConfig
    )


class MemoryEnhancePlugin(MaiBotPlugin):
    """记忆增强：增强版记忆检索（跨聊天流 / 事实 / 日记）+ 模型日记工具。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = MemoryEnhanceConfig

    # 订阅宿主全局配置广播：bot 配置变化时刷新「宿主检索参数」只读快照
    config_reload_subscriptions: ClassVar[Tuple[str, ...]] = ("bot",)

    def __init__(self) -> None:
        super().__init__()
        self._fact_db_checked: bool = False
        self._fact_db_path: Optional[Path] = None
        self._fact_db_warned: bool = False
        self._host_threshold_values: Dict[str, Any] = {}
        self._diary_store: Optional[DiaryStore] = None
        self._tool_definition_cache: Dict[str, Dict[str, Any]] = {}
        self._group_stream_cache: List[str] = []
        self._group_stream_fetched_at: float = 0.0
        self._scope_route_truncated: bool = False

    # ---------------------------------------------------------- 生命周期

    async def on_load(self) -> None:
        """加载：工具模式自检 + 读取宿主阈值快照（供只读展示）。"""
        await self._check_tool_mode()
        await self._refresh_host_threshold_snapshot()

    async def on_unload(self) -> None:
        self.ctx.logger.info("记忆增强已卸载")

    async def on_config_update(
        self, scope: str, config_data: dict[str, Any], version: str
    ) -> None:
        """配置热更新：应用新配置并重置依赖配置的缓存。

        宿主加载器强制要求覆盖本方法（SDK 基类默认实现抛 NotImplementedError，
        plugin_loader.py 的 _validate_sdk_plugin_contract 会拒绝未覆盖的插件）。
        scope=self 为插件自身配置，应用后重置 metadata.db 路径探测缓存
        （storage.data_dir 可能已变化）。
        """
        if scope == CONFIG_RELOAD_SCOPE_SELF:
            self.set_plugin_config(config_data)
            self._fact_db_checked = False
            self._fact_db_path = None
            self.ctx.logger.info(
                f"配置已更新（version={version}），事实库路径将重新探测"
            )
        else:
            del config_data, version
        # 宿主配置可能变化（threshold 等）：刷新只读展示快照
        await self._refresh_host_threshold_snapshot()

    async def _check_tool_mode(self) -> None:
        """按「工具」页签的接入方式做加载自检。

        替换原生模式：内置 query_memory 未禁用时，插件同名工具会被内置 provider
        跳过（宿主工具注册表同名先注册者优先、内置 provider 先注册，见
        standards/host-behavior.md §11）——禁用内置是使用该模式的必要前置条件。
        新增工具模式（默认）：无前置条件；内置 query_memory 由 planner 钩子从
        列表面移除（LLM 看不见就不会调用），无需用户改宿主配置。
        """
        replace_builtin = bool(self.config.tools.replace_builtin)
        try:
            enabled = await self.ctx.config.get(_BUILTIN_TOOL_SWITCH)
        except Exception as exc:
            self.ctx.logger.info(f"内置工具自检跳过（读取配置失败: {exc}）")
            return
        if enabled is None:
            self.ctx.logger.info("内置工具自检跳过（未读取到开关值）")
            return
        if replace_builtin:
            if bool(enabled):
                self.ctx.logger.warning(
                    "「替换原生」已开启，但宿主内置「启用记忆检索」仍处于开启状态"
                    "（a_memorix.integration.enable_memory_query_tool = true）："
                    "本插件的同名 query_memory 会被内置版跳过、不会生效。"
                    "请在宿主 WebUI「记忆」页关闭「启用记忆检索」后重启，"
                    "或改用默认的「新增工具」模式（search_memory）。"
                )
            else:
                self.ctx.logger.info("记忆增强已就绪（query_memory 同名接管生效）")
        else:
            extra = (
                "（内置 query_memory 仍启用，将由插件从 planner 列表移除）"
                if bool(enabled)
                else ""
            )
            self.ctx.logger.info(f"记忆增强已就绪（search_memory 工具模式）{extra}")

    async def _refresh_host_threshold_snapshot(self) -> None:
        """读取宿主 A_Memorix 阈值配置（只读展示用；失败静默为 None）。"""
        values: Dict[str, Any] = {}
        for key, path in _HOST_THRESHOLD_KEYS.items():
            try:
                values[key] = await self.ctx.config.get(path)
            except Exception:
                values[key] = None
        self._host_threshold_values = values

    # ---------------------------------------------------------- WebUI

    def get_webui_config_schema(
        self,
        *,
        plugin_id: str = "",
        plugin_name: str = "",
        plugin_version: str = "",
        plugin_description: str = "",
        plugin_author: str = "",
    ) -> Dict[str, Any]:
        """配置 Schema：把宿主阈值快照写入「宿主检索参数」只读字段。"""
        schema = super().get_webui_config_schema(
            plugin_id=plugin_id,
            plugin_name=plugin_name,
            plugin_version=plugin_version,
            plugin_description=plugin_description,
            plugin_author=plugin_author,
        )
        try:
            section = (schema.get("sections") or {}).get("host_retrieval") or {}
            fields = section.get("fields") or {}
            for key, value in (self._host_threshold_values or {}).items():
                field = fields.get(key)
                if isinstance(field, dict) and value is not None:
                    field["default"] = value
        except Exception:
            pass
        return schema

    # ---------------------------------------------------------- planner 钩子

    @HookHandler(
        "maisaka.planner.before_request",
        name="memory_enhance_tools",
        description="按配置暴露记忆检索与日记工具、滤除内置 query_memory",
        mode=HookMode.BLOCKING,
        order=HookOrder.EARLY,
        timeout_ms=3000,
        error_policy=ErrorPolicy.SKIP,
    )
    async def handle_planner_before_request(self, **kwargs: Any) -> Dict[str, Any]:
        """planner 工具列表维护（standards/host-behavior.md §11/§12 手法）。

        - 新增工具模式（默认）：从 planner 列表移除内置 query_memory——LLM 看不见
          就不会调用（§11 物理移除）；补回 search_memory 常驻定义（始终发现）；
        - 替换原生模式：内置 query_memory 仍启用（列表中存在）时不补回插件版——
          同名调用执行的是内置 handler，补回增强描述会造成「看到的描述」与
          「实际执行的实现」不一致；内置关闭（列表中没有）后补回插件版；
        - 日记：开启时补回 write_diary 常驻定义。
        """
        tools = kwargs.get("tool_definitions")
        if not isinstance(tools, list):
            return {"action": "continue"}

        updated: List[Any] = tools
        changed = False

        if self.config.tools.replace_builtin:
            if not self._tool_present(updated, "query_memory"):
                updated, added = self._ensure_tool_definition(
                    updated, self.handle_query_memory
                )
                changed = changed or added
        else:
            if self._tool_present(updated, "query_memory"):
                updated = [
                    tool
                    for tool in updated
                    if self._tool_function_name(tool) != "query_memory"
                ]
                changed = True
            updated, added = self._ensure_tool_definition(
                updated, self.handle_search_memory
            )
            changed = changed or added

        if self.config.diary.enabled:
            updated, added = self._ensure_tool_definition(
                updated, self.handle_write_diary
            )
            changed = changed or added

        if not changed:
            return {"action": "continue"}
        return {
            "action": "continue",
            "modified_kwargs": {**kwargs, "tool_definitions": updated},
        }

    @staticmethod
    def _tool_function_name(tool: Any) -> str:
        """从 LLM 工具定义中取函数名（结构不符返回空串）。"""
        if not isinstance(tool, dict):
            return ""
        function = tool.get("function")
        if not isinstance(function, dict):
            return ""
        return str(function.get("name") or "").strip()

    def _tool_present(self, tools: List[Any], name: str) -> bool:
        """工具列表中是否已存在指定工具。"""
        return any(self._tool_function_name(tool) == name for tool in tools)

    def _ensure_tool_definition(
        self, tools: List[Any], handler: Any
    ) -> Tuple[List[Any], bool]:
        """把 deferred 声明工具的 LLM 定义补回列表（已在则原样返回）。

        返回 ``(工具列表, 是否追加)``；定义生成失败时原样返回（该轮不补回）。
        """
        info = getattr(handler, "__maibot_component_info__", None)
        name = str(getattr(info, "name", "") or "").strip()
        if not name or self._tool_present(tools, name):
            return tools, False
        definition = self._build_tool_definition(name, info)
        if definition is None:
            return tools, False
        return [*tools, definition], True

    def _build_tool_definition(
        self, name: str, info: Any
    ) -> Optional[Dict[str, Any]]:
        """从组件声明生成 LLM 工具定义（懒生成 + 缓存；失败缓存空值防重试）。"""
        cached = self._tool_definition_cache.get(name)
        if cached is not None:
            return cached or None
        try:
            description = str(
                getattr(info, "brief_description", "")
                or getattr(info, "description", "")
                or ""
            ).strip()
            definition: Dict[str, Any] = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": self._tool_parameters_schema(
                        getattr(info, "parameters", None)
                    ),
                },
            }
            self._tool_definition_cache[name] = definition
            return definition
        except Exception as exc:
            self.ctx.logger.info(f"生成工具定义失败（{name}）: {exc}")
            self._tool_definition_cache[name] = {}
            return None

    @staticmethod
    def _tool_parameters_schema(parameters: Any) -> Dict[str, Any]:
        """ToolParameterInfo 列表 → JSON Schema（补回定义的参数部分）。"""
        properties: Dict[str, Any] = {}
        required: List[str] = []
        for param in parameters or []:
            pname = str(getattr(param, "name", "") or "").strip()
            if not pname:
                continue
            param_type = getattr(param, "param_type", None)
            type_value = (
                getattr(param_type, "value", None)
                or getattr(param_type, "name", None)
                or "string"
            )
            entry: Dict[str, Any] = {"type": str(type_value)}
            desc = str(getattr(param, "description", "") or "").strip()
            if desc:
                entry["description"] = desc
            enum_values = getattr(param, "enum_values", None)
            if enum_values:
                entry["enum"] = list(enum_values)
            properties[pname] = entry
            if bool(getattr(param, "required", False)):
                required.append(pname)
        schema: Dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        return schema

    # ---------------------------------------------------------- 工具

    @Tool(
        "search_memory",
        # 可见性基态 deferred + planner 钩子按配置补回常驻定义（standards/
        # host-behavior.md §12：可见性无运行时切换 API，「deferred 基态 + 补回」
        # 是实现「始终发现」的既定手法；reply-control 的 select_emoji 为先例）。
        visibility="deferred",
        description=(
            "检索长期记忆（可含事实声明、日记与跨聊天流）。"
            "每次查询只聚焦一个主题或一个关键词；多个关键词拆成多次调用。"
            "模糊主题短语即可，不必是精确关键词。"
            "检索范围与数据源由插件配置决定（跨聊天流默认关闭）。"
            "想记东西时用 write_diary；查人物画像/档案请用 query_person_profile。"
        ),
        parameters=_search_tool_parameters(),
    )
    async def handle_search_memory(
        self,
        stream_id: str = "",
        query: str = "",
        limit: Any = None,
        include_fact_evidence: Any = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """search_memory：默认工具模式的入口。"""
        del kwargs
        return await self._run_memory_search(
            stream_id, query, limit, include_fact_evidence
        )

    @Tool(
        "query_memory",
        # 同名接管模式（配置「替换原生」开启时由 planner 钩子补回常驻定义）：
        # 同名工具仅在宿主内置 query_memory 关闭后才能真正生效（宿主工具注册表
        # 同名先注册者优先、内置 provider 先注册，见 standards/host-behavior.md §11）。
        visibility="deferred",
        description=(
            "检索长期记忆（可含事实声明、日记与跨聊天流）。"
            "每次查询只聚焦一个主题或一个关键词；多个关键词拆成多次调用。"
            "模糊主题短语即可，不必是精确关键词。"
            "检索范围与数据源由插件配置决定（跨聊天流默认关闭）。"
            "想记东西时用 write_diary；查人物画像/档案请用 query_person_profile。"
        ),
        parameters=_search_tool_parameters(),
    )
    async def handle_query_memory(
        self,
        stream_id: str = "",
        query: str = "",
        limit: Any = None,
        include_fact_evidence: Any = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """query_memory：同名接管模式的入口（与 search_memory 同实现）。"""
        del kwargs
        return await self._run_memory_search(
            stream_id, query, limit, include_fact_evidence
        )

    @Tool(
        "write_diary",
        # 可见性基态 deferred + planner 钩子按开关补回常驻定义（「随时记录」
        # 要求工具始终在列表；关闭开关后不补回，handler 内亦会拒绝调用）。
        visibility="deferred",
        description=(
            "写日记：把你想记住的内容（想法、心情、判断、发生的事）记下来长期保存，"
            "以后检索记忆时可以回看。随时可用，想记什么就记什么；"
            "一次写一条，写清楚发生了什么和自己的感受。"
        ),
        parameters=[
            ToolParameterInfo(
                name="content",
                param_type=ToolParamType.STRING,
                description="日记内容（自由书写，一次一条）。",
                required=True,
            ),
        ],
    )
    async def handle_write_diary(
        self, stream_id: str = "", content: str = "", **kwargs: Any
    ) -> Dict[str, Any]:
        """write_diary：追加一条日记（本插件自有库，与宿主记忆库分离）。

        ``stream_id`` 由宿主注入、不暴露给模型：日记自动归属写入时的聊天流，
        检索可见范围跟随「跨聊天流」开关（默认仅当前流）。
        """
        del kwargs

        if not self.config.diary.enabled:
            # deferred 基态下即使开关关闭，工具仍可能在 deferred 池被
            # tool_search 发现，必须在 handler 内拒绝调用（同 select_emoji 先例）
            return {"success": False, "error": "日记功能已在配置中关闭"}

        text = str(content or "").strip()
        if not text:
            return {"success": False, "error": "缺少必要参数 content（要记录的内容）"}

        truncated = False
        if len(text) > _DIARY_MAX_CHARS:
            text = text[:_DIARY_MAX_CHARS]
            truncated = True

        clean_stream = str(stream_id or "").strip()
        try:
            store = self._get_diary_store()
            entry_id, created_at = store.add(text, stream_id=clean_stream)
        except Exception as exc:
            self.ctx.logger.info(f"日记写入失败: {exc}")
            return {"success": False, "error": f"日记写入失败：{exc}"}

        local_time = format_local_time(created_at)
        self.ctx.logger.info(
            f"日记已记录（#{entry_id}，{local_time}，流 {clean_stream or '∅'}）"
        )
        notice = "（内容超长已截断）" if truncated else ""
        return {
            "success": True,
            "content": f"[日记] 已记录（{local_time}）{notice}",
            "entry_id": entry_id,
            "created_at": created_at,
        }

    # ---------------------------------------------------------- 检索主流程

    async def _run_memory_search(
        self,
        stream_id: str,
        query: str,
        limit: Any,
        include_fact_evidence: Any,
    ) -> Dict[str, Any]:
        """增强版长期记忆检索：按配置范围取四路数据源并合并输出。"""
        clean_query = str(query or "").strip()
        if not clean_query:
            return {"success": False, "error": "缺少必要参数 query（单一主题或关键词）"}

        try:
            limit_value = max(1, min(20, int(limit)))
        except (TypeError, ValueError):
            limit_value = 5

        sources = self.config.sources
        enabled = {
            "facts": bool(sources.facts),
            "diaries": bool(sources.diaries),
            "paragraphs_relations": bool(sources.paragraphs_relations),
            "episodes": bool(sources.episodes),
        }
        if not any(enabled.values()):
            return {"success": False, "error": "全部数据源均已关闭：请至少开启一路数据源"}

        cross_scope = str(self.config.search.cross_scope or "关闭")
        route_ids = await self._resolve_scope_routes(stream_id, cross_scope)
        if route_ids is not None and not route_ids:
            # 需要流范围却拿不到任何可用流：拒绝降级（空 chat_id 会变成全库检索，违背隐私默认）
            return {"success": False, "error": "无法确定当前聊天流（缺少 stream_id）"}
        allowed_chat_ids = (
            None if route_ids is None else {item for item in route_ids if item}
        )

        scope_label = (
            "全库"
            if route_ids is None
            else (f"仅群聊({len(route_ids)}路)" if cross_scope == "仅群聊" else "本流")
        )
        self.ctx.logger.info(
            "记忆检索[{}]: query={!r} limit={} 源={}".format(
                scope_label,
                clean_query[:60],
                limit_value,
                ",".join(key for key, value in enabled.items() if value),
            )
        )

        sections: List[str] = []
        errors: List[str] = []

        if enabled["facts"]:
            ok, payload = await self._search_facts(
                clean_query,
                limit_value,
                allowed_chat_ids=allowed_chat_ids,
                include_evidence=bool(include_fact_evidence),
            )
            if ok:
                if payload:
                    sections.append(payload)
            else:
                errors.append(payload)

        if enabled["diaries"]:
            ok, payload = await self._search_diary(
                clean_query,
                limit_value,
                stream_ids=allowed_chat_ids,
            )
            if ok:
                if payload:
                    sections.append(payload)
            else:
                errors.append(payload)

        if enabled["paragraphs_relations"]:
            ok, payload = await self._capability_search(
                "search",
                "记忆",
                clean_query,
                limit_value,
                route_ids=route_ids,
            )
            if ok:
                if payload:
                    sections.append(payload)
            else:
                errors.append(payload)

        if enabled["episodes"]:
            ok, payload = await self._capability_search(
                "episode",
                "经历",
                clean_query,
                limit_value,
                route_ids=route_ids,
            )
            if ok:
                if payload:
                    sections.append(payload)
            else:
                errors.append(payload)

        # 事实路已在最前追加（精确命中优先置顶）；其余按宿主返回顺序
        content = "\n".join(sections).strip()
        if not content and errors:
            return {"success": False, "error": f"长期记忆检索失败：{'; '.join(errors)}"}
        if errors:
            note = f"（部分数据源失败：{'; '.join(errors)}）"
            content = f"{content}\n{note}" if content else note
        if not content:
            return {
                "success": True,
                "content": f"未找到与「{clean_query}」相关的长期记忆。",
            }
        return {"success": True, "content": content}

    async def _resolve_scope_routes(
        self, stream_id: str, cross_scope: str
    ) -> Optional[List[str]]:
        """解析检索范围：返回需要逐流检索的 chat 列表（None = 全库单次调用）。

        - 关闭：仅当前流；
        - 仅群聊：当前流 + 全部群聊（去重保序；不含其它私聊——私聊记忆永不跨流）；
        - 全部：None（chat_id 置空的全库单次调用）。
        """
        if cross_scope == "全部":
            return None
        current = str(stream_id or "").strip()
        if cross_scope != "仅群聊":
            return [current] if current else []
        routes: List[str] = [current] if current else []
        for group_id in await self._group_stream_ids():
            if group_id and group_id not in routes:
                routes.append(group_id)
        if len(routes) > _MAX_SCOPE_ROUTES:
            routes = routes[:_MAX_SCOPE_ROUTES]
            if not self._scope_route_truncated:
                self._scope_route_truncated = True
                self.ctx.logger.warning(
                    f"「仅群聊」检索路数超过 {_MAX_SCOPE_ROUTES}，已截断"
                    "（本轮仅覆盖当前流与部分群聊）"
                )
        return routes

    async def _group_stream_ids(self) -> List[str]:
        """群聊流 session_id 列表（带 TTL 缓存；获取失败沿用旧缓存）。

        记忆归属 key 与内置 query_memory 的 chat_id 同源（均为 session_id），
        因此这里枚举出的 id 可直接用作 knowledge.search 的 chat_id。
        """
        now = time.monotonic()
        if (
            self._group_stream_cache
            and now - self._group_stream_fetched_at < _GROUP_STREAM_CACHE_SECONDS
        ):
            return self._group_stream_cache
        try:
            streams = await self.ctx.chat.get_group_streams(_ALL_PLATFORMS)
        except Exception as exc:
            self.ctx.logger.info(f"获取群聊流列表失败: {exc}")
            return self._group_stream_cache
        if isinstance(streams, dict):
            if streams.get("success") is False:
                self.ctx.logger.info(
                    "获取群聊流列表被宿主拒绝"
                    f"（error={streams.get('error')!r}）；请确认 manifest "
                    "capabilities 已声明 chat.get_group_streams"
                )
                return self._group_stream_cache
            nested = streams.get("streams")
            streams = nested if isinstance(nested, list) else []
        if not isinstance(streams, list):
            return self._group_stream_cache
        ids: List[str] = []
        for item in streams:
            if not isinstance(item, dict):
                continue
            session_id = str(item.get("session_id") or "").strip()
            group_id = str(item.get("group_id") or "").strip()
            if session_id and group_id and session_id not in ids:
                ids.append(session_id)
        self._group_stream_cache = ids
        self._group_stream_fetched_at = now
        return ids

    # ---------------------------------------------------------- 段落/关系路 与 经历路

    async def _capability_search(
        self,
        mode: str,
        label: str,
        query: str,
        limit: int,
        *,
        route_ids: Optional[List[str]],
    ) -> Tuple[bool, str]:
        """调用宿主 knowledge.search（mode 区分段落/关系路与经历路）。

        范围语义：``route_ids=None`` = 全库单次调用（「全部」模式）；单元素
        = 仅该流（本流语义）；多元素 = 逐流并发检索后合并去重（「仅群聊」模式）。
        返回 (ok, payload)：ok=False 时 payload 为失败原因；ok=True 时 payload 为
        带源标注的文本（空串表示该路无结果）。
        """
        if route_ids is None:
            ok, text = await self._capability_search_once(
                mode, query, limit, chat_id="", respect_filter=False
            )
            if not ok:
                return False, f"{label}路失败: {text}"
            return (True, f"[{label}] {text}") if text else (True, "")

        if len(route_ids) <= 1:
            chat_id = route_ids[0] if route_ids else ""
            ok, text = await self._capability_search_once(
                mode, query, limit, chat_id=chat_id, respect_filter=True
            )
            if not ok:
                return False, f"{label}路失败: {text}"
            return (True, f"[{label}] {text}") if text else (True, "")

        per_route = max(1, limit // len(route_ids))
        results = await asyncio.gather(
            *(
                self._capability_search_once(
                    mode, query, per_route, chat_id=route_id, respect_filter=False
                )
                for route_id in route_ids
            )
        )
        texts: List[str] = []
        failed = 0
        for ok, text in results:
            if not ok:
                failed += 1
                continue
            if text:
                texts.append(text)
        merged: List[str] = []
        seen: set[str] = set()
        for text in texts:
            for line in text.splitlines():
                clean_line = line.strip()
                if clean_line and clean_line not in seen:
                    seen.add(clean_line)
                    merged.append(clean_line)
        if not merged:
            if failed == len(results):
                return False, f"{label}路失败: 全部 {failed} 路检索均失败"
            return True, ""
        return True, f"[{label}] " + "\n".join(merged)

    async def _capability_search_once(
        self,
        mode: str,
        query: str,
        limit: int,
        *,
        chat_id: str,
        respect_filter: bool,
    ) -> Tuple[bool, str]:
        """单次 knowledge.search 调用；返回 (ok, 剥前缀后的正文 / 失败原因)。"""
        try:
            result = await self.ctx.call_capability(
                "knowledge.search",
                query=query,
                limit=limit,
                mode=mode,
                chat_id=chat_id,
                respect_filter=respect_filter,
            )
        except Exception as exc:
            self.ctx.logger.info(f"记忆检索异常（{mode}）: {exc}")
            return False, str(exc)

        ok, payload_text = self._parse_capability_result(result)
        if not ok:
            self.ctx.logger.info(f"记忆检索失败（{mode}）: {payload_text}")
            return False, payload_text

        text = self._strip_capability_prefix(payload_text)
        if self._is_capability_miss(text):
            # 宿主无命中时返回礼貌话术（你不太了解有关…的知识），视为该路无结果
            return True, ""
        return True, text

    @staticmethod
    def _parse_capability_result(result: Any) -> Tuple[bool, str]:
        """解析宿主能力返回（兼容 SDK 各版本的包装差异）。

        观测到的形态（2026-09-18 部署实测）：
        - ``str``：SDK 已把成功响应的 content 解包为纯文本（部分 SDK 版本对
          知识能力做了解包）；
        - ``dict`` + ``success`` / ``content``：原始包装（成功）；
        - ``dict`` + ``error``：原始失败包装；
        - 其它类型：失败，并携带类型与原始值（避免只报“未知错误”）。
        """
        if isinstance(result, str):
            return True, result
        if isinstance(result, dict):
            error = str(result.get("error") or "").strip()
            if error and not result.get("success"):
                return False, error
            if result.get("success") or "content" in result:
                return True, str(result.get("content") or "")
            return False, error or "未知错误"
        return False, f"意外的返回类型 {type(result).__name__}: {result!r}"

    @staticmethod
    def _is_capability_miss(text: str) -> bool:
        """宿主无命中时返回礼貌话术（如「你不太了解有关 x 的知识」），视为无结果。"""
        return str(text or "").startswith(_CAPABILITY_MISS_PREFIX)

    # ---------------------------------------------------------- 事实路（metadata.db 直读）

    async def _search_facts(
        self,
        query: str,
        limit: int,
        *,
        allowed_chat_ids: Optional[set[str]],
        include_evidence: bool,
    ) -> Tuple[bool, str]:
        """只读直读 fact_claims：状态/过期/范围过滤 + LIKE 匹配 + 打分排序。"""
        path = await self._resolve_metadata_db()
        if path is None:
            self._warn_fact_db_missing()
            return True, ""

        try:
            rows = self._query_fact_claims(path)
        except Exception as exc:
            self.ctx.logger.info(f"事实路查询失败: {exc}")
            return False, f"事实路失败: {exc}"

        terms = [term for term in _TERM_SPLIT_PATTERN.split(query) if term.strip()]
        if not terms:
            terms = [query]

        now = time.time()
        matched: List[Tuple[int, float, float, Dict[str, Any]]] = []
        for row in rows:
            if not self._fact_in_scope(row, allowed_chat_ids=allowed_chat_ids):
                continue
            valid_to = row.get("valid_to")
            if valid_to is not None and float(valid_to) <= now:
                continue
            haystack = f"{row.get('fact_key') or ''} {row.get('value_text') or ''}".lower()
            score = sum(1 for term in terms if term.lower() in haystack)
            if score <= 0:
                continue
            try:
                confidence = float(row.get("confidence") or 0.0)
                confirmed_at = float(row.get("last_confirmed_at") or 0.0)
            except (TypeError, ValueError):
                confidence, confirmed_at = 0.0, 0.0
            matched.append((score, confidence, confirmed_at, row))

        if not matched:
            return True, ""
        matched.sort(key=lambda item: (-item[0], -item[1], -item[2]))
        selected = [item[3] for item in matched[:limit]]

        evidence_counts: Dict[str, int] = {}
        if include_evidence:
            evidence_counts = self._query_evidence_counts(
                path, [str(row.get("claim_id") or "") for row in selected]
            )

        lines = [
            self._render_fact(
                row,
                evidence_counts.get(str(row.get("claim_id") or ""))
                if include_evidence
                else None,
            )
            for row in selected
        ]
        return True, "\n".join(lines)

    async def _resolve_metadata_db(self) -> Optional[Path]:
        """定位 A_Memorix 的 metadata.db（探测一次并缓存；先例：person-alias）。"""
        if self._fact_db_checked:
            return self._fact_db_path
        self._fact_db_checked = True

        data_dir = ""
        try:
            value = await self.ctx.config.get("a_memorix.storage.data_dir", "")
            data_dir = str(value or "").strip()
        except Exception as exc:
            self.ctx.logger.info(f"读取 a_memorix.storage.data_dir 失败: {exc}")

        candidates: List[Path] = []
        if data_dir:
            candidates.append(Path(data_dir) / "metadata" / "metadata.db")
        candidates.append(Path("data") / "a-memorix" / "metadata" / "metadata.db")
        candidates.append(Path("data") / "metadata" / "metadata.db")

        for candidate in candidates:
            try:
                if candidate.is_file():
                    self._fact_db_path = candidate.resolve()
                    break
            except OSError:
                continue
        return self._fact_db_path

    def _warn_fact_db_missing(self) -> None:
        """metadata.db 不存在时提示一次（不刷屏）。"""
        if self._fact_db_warned:
            return
        self._fact_db_warned = True
        self.ctx.logger.warning(
            "事实路未找到 metadata.db（预期位置 data/a-memorix/metadata/metadata.db）"
            "：该路暂时跳过；如 A_Memorix 数据目录为自定义位置，请确认其 storage.data_dir 配置。"
        )

    @staticmethod
    def _query_fact_claims(path: Path) -> List[Dict[str, Any]]:
        """只读查询有效状态的事实声明（claims 数百量级，全扫无压力）。"""
        connection = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro", uri=True, timeout=1.5
        )
        try:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT claim_id, fact_key, value_text, scope_type, scope_id, "
                "status, confidence, valid_to, last_confirmed_at "
                "FROM fact_claims WHERE status = ?",
                (_FACT_STATUS_ACTIVE,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    @staticmethod
    def _query_evidence_counts(path: Path, claim_ids: List[str]) -> Dict[str, int]:
        """按需查询命中事实的证据条数（fact_evidence 关联计数）。"""
        ids = [claim_id for claim_id in claim_ids if claim_id]
        if not ids:
            return {}
        connection = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro", uri=True, timeout=1.5
        )
        try:
            placeholders = ",".join("?" for _ in ids)
            rows = connection.execute(
                "SELECT claim_id, COUNT(*) AS n FROM fact_evidence "
                f"WHERE claim_id IN ({placeholders}) GROUP BY claim_id",
                ids,
            ).fetchall()
            return {str(row[0]): int(row[1]) for row in rows}
        finally:
            connection.close()

    @staticmethod
    def _fact_in_scope(
        row: Dict[str, Any], *, allowed_chat_ids: Optional[set[str]]
    ) -> bool:
        """事实范围过滤：person 级视为全局知识（宿主 query_person_profile 先例）；
        chat 级限检索范围集合（None = 全库「全部」模式，不限制）。"""
        if allowed_chat_ids is None:
            return True
        scope_type = str(row.get("scope_type") or "").strip().lower()
        if scope_type == "person":
            return True
        if scope_type == "chat":
            return str(row.get("scope_id") or "").strip() in allowed_chat_ids
        return False

    @staticmethod
    def _render_fact(row: Dict[str, Any], evidence_count: Optional[int]) -> str:
        """渲染单条事实：`[事实] value（可信度 x.xx，证据 N 条）`。

        fact_key 是机器键（实测为 ``statement:<sha256>`` 形态），对人类与 planner
        均无价值，不展示；仅当 value_text 为空时回退展示 fact_key（便于排查脏数据）。
        """
        fact_key = str(row.get("fact_key") or "").strip()
        value_text = str(row.get("value_text") or "").strip()
        body = value_text or fact_key

        extras: List[str] = []
        try:
            confidence = float(row.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence > 0:
            extras.append(f"可信度 {confidence:.2f}")
        if evidence_count:
            extras.append(f"证据 {evidence_count} 条")
        suffix = f"（{'，'.join(extras)}）" if extras else ""
        return f"[事实] {body}{suffix}"

    # ---------------------------------------------------------- 日记路（本插件自有库）

    def _get_diary_store(self) -> DiaryStore:
        """懒初始化日记库（插件数据目录，首次访问时创建目录与库文件）。"""
        if self._diary_store is None:
            data_dir = self.ctx.paths.data_dir
            data_dir.mkdir(parents=True, exist_ok=True)
            self._diary_store = DiaryStore(data_dir / _DIARY_DB_FILENAME)
        return self._diary_store

    async def _search_diary(
        self,
        query: str,
        limit: int,
        *,
        stream_ids: Optional[set[str]],
    ) -> Tuple[bool, str]:
        """检索日记库：关键词匹配 + 时间倒序，渲染为 ``[日记] 时间 内容``。

        可见范围跟随「跨聊天流检索」设置：关闭=当前流；仅群聊=当前流+全部群聊；
        全部=None 全收——与段落/事实路的隐私语义一致。
        """
        try:
            store = self._get_diary_store()
            terms = [term for term in _TERM_SPLIT_PATTERN.split(query) if term.strip()]
            if not terms:
                terms = [query]
            rows = store.search(terms, limit, stream_ids=stream_ids)
        except Exception as exc:
            self.ctx.logger.info(f"日记路查询失败: {exc}")
            return False, f"日记路失败: {exc}"
        if not rows:
            return True, ""
        lines = [
            "[日记] {} {}".format(
                format_local_time(row.get("created_at") or 0.0),
                " ".join(str(row.get("content") or "").split()),
            )
            for row in rows
        ]
        return True, "\n".join(lines)

    @staticmethod
    def _strip_capability_prefix(content: str) -> str:
        """剥离宿主能力的包装前缀（“你知道这些知识: ”），保留检索正文。"""
        text = str(content or "").strip()
        if text.startswith(_CAPABILITY_PREFIX):
            text = text[len(_CAPABILITY_PREFIX):].strip()
        return text


def create_plugin() -> MemoryEnhancePlugin:
    """插件工厂函数，由 SDK Runner 调用。"""
    return MemoryEnhancePlugin()
