# 更新日志

## 0.2.0（2026-09-18）

- 项目更名：记忆检索增强 → **记忆增强**（目录与插件 ID：`memory-enhance`）；
- 工具接入方式可切换（配置「工具」页签）：
  - 「新增工具」（默认）：`search_memory` 始终随工具列表提供，planner 钩子自动从列表移除
    内置 `query_memory`——无需任何宿主配置，装上即用；
  - 「替换原生」：`query_memory` 同名接管（需先在宿主关闭内置记忆检索）；
  - 两个检索工具共享实现，均以 `visibility="deferred"` + planner 钩子按配置补回常驻定义
    （standards/host-behavior.md §11/§12 的既有手法）；
- 新增**日记**：`write_diary` 工具——模型随时记录想记住的内容（本插件自有 SQLite，
  独立于宿主记忆库）；检索时作为第四路数据源以 `[日记] 本地时间 内容` 回看；
  日记自动归属写入时的聊天流，检索范围跟随「跨聊天流检索」开关（默认仅当前流，
  与其它数据源的隐私语义一致；旧库自动迁移补列）；
- 数据源页签新增「日记」开关；日记写入返回带 `[日记]` 前缀与记录时间；
- 「跨聊天流检索」由开关改为**三态下拉**：关闭（默认）/ 仅群聊 / 全部——「仅群聊」经
  `chat.get_group_streams` 枚举群聊流逐流检索后合并去重，**私聊记忆在任何模式下都不跨流**；
  manifest 补声明 `chat.get_group_streams` 能力。

## 0.1.0（2026-09-18）

- 首版：增强版 `query_memory` 工具，同名接管宿主内置检索（需先在宿主配置中禁用内置
  `a_memorix.integration.enable_memory_query_tool`，插件加载时自检并提示）；
- 三路数据源（各自独立开关）：段落/关系路（宿主 `knowledge.search`）、事实路
  （只读直读 `metadata.db` 的 `fact_claims`：active 状态 + `valid_to` 过期过滤 +
  person/chat 范围过滤 + LIKE 匹配 + 可信度排序）、经历路（`mode=episode`，默认关）；
- 跨聊天流检索开关（默认关；开启后 `chat_id` 置空 = 全库，与宿主启发式记忆召回同款语义）；
- 只读插件：不写入任何记忆数据，不新增 embedding 调用。
