# 📊 Full-Stack A/B Testing Data Platform (GrowthBook + StarRocks)

This project builds an enterprise-grade, modular A/B testing analytics platform based on GrowthBook experiment configuration and StarRocks data warehouse. It supports a full-cycle workflow—from experiment metadata ingestion, user behavior aggregation, and modeling analysis to automated report generation and integration with Metabase dashboards. The platform supports multi-team, multi-business scenarios such as Ads, Subscription, Retention, and Chatbot optimization.

---

## 1️⃣ Experiment Metadata Ingestion

* The platform integrates with the **GrowthBook API** to automatically fetch experiment definitions, variation structure, traffic split, and metric settings;
* All experiment metadata and user variant assignments are stored in a centralized `experiment_data` table in StarRocks;
* The system uses `tag` as the **core orchestration unit**, representing a logical group of experiments under a business module—not an individual experiment or version.

### 🧠 Accurate Definition of `tag`:

* Each `tag` typically maps to a **business team or functional area** (e.g., `retention`, `subscribe`, `chat_entry`);
* For each tag, the system fetches all associated experiments and automatically selects the **latest active phase** (start/end time, variation structure) as the basis for analysis;
* Multiple experiments may coexist under the same tag, but **only the latest configuration is used**, and results are **automatically overwritten** in report tables—ensuring Metabase dashboards always reflect the most recent outcome.

---

## 2️⃣ User-Level Wide Table Construction (Dynamic by tag + event\_date)

* Built on StarRocks’ real-time engine, the platform aggregates key user behaviors during the experiment window (e.g., clicks, engagement, subscriptions, payments);
* The system dynamically generates wide tables based on `tag` and `event_date` naming conventions (e.g., `tbl_wide_user_retention_retention`);
* Field names are standardized across modules, enabling cross-team reuse and version control;
* Supports both daily incremental and full-period data processing, facilitating flexible orchestration, debugging, and backfill.

---

## 3️⃣ Experiment Modeling & Evaluation

* The platform supports multiple evaluation methods:

  * ✅ Bayesian inference (posterior mean / win probability)
  * ✅ Uplift modeling (net incremental impact)
  * ✅ t-test with confidence interval estimation
* Final results are written to standardized report tables (e.g., `tbl_report_user_retention_<tag>`), schema-aligned with Metabase dashboards;
* Each run automatically **overwrites previous records**, ensuring the dashboard is always accurate, up-to-date, and deduplicated.

---

## 4️⃣ Modular Architecture & Task Decoupling

* Each business module (Retention, Subscribe, Recharge, etc.) is organized in an independent directory that encapsulates its full ETL logic;
* The main entry script `main_run.py --tag retention` can execute any specific module by tag;
* All logic is modularized and linked via tag—ensuring full decoupling across teams, pipelines, and outputs;
* Supports single-module execution, full-batch orchestration, or integration into any scheduling system.

---

## 📁 Project Structure

```
state3/
├── Advertisement/         # Ads-related experiments
├── Retention/             # User retention experiments
├── Subscribe/             # Subscription conversion
├── Recharge/              # Payment/monetization tracking
├── Engagement/            # Chatbot engagement metrics
├── growthbook_fetcher/    # GrowthBook API integration
├── main_all.py            # Execute all tags
├── main_run.py            # Execute one tag for testing/debugging
└── README.md
```

---

## 💾 Output Tables (Dynamically named by tag)

| Table Name                                     | Description                                                        |
| ---------------------------------------------- | ------------------------------------------------------------------ |
| `experiment_data`                              | Stores experiment config and variant assignment from GrowthBook    |
| `tbl_wide_user_<module>_<tag>`                 | User behavior wide table (clicks, subscriptions, retention, etc.)  |
| `tbl_report_user_<module>_<tag>`               | Final report table with significance, uplift, and Bayesian results |
| `flow_report_app.tbl_report_ab_testing_result` | Aggregated Metabase dashboard table (overwritten by tag)           |

---

## 🎯 Business Value

* 📌 Fully automated pipeline—no manual SQL or notebooks needed; 5x faster analysis cycles;
* 📌 Unified platform architecture supporting collaboration across multiple teams and modules;
* 📌 Results automatically published to Metabase-compatible tables for real-time monitoring;
* 📌 Tag-based architecture enables scalable integration and stable expansion.

---

## 🧠 Technical Highlights

| Feature      | Details                                                                        |
| ------------ | ------------------------------------------------------------------------------ |
| Data Access  | GrowthBook API for experiment definitions & variant mapping                    |
| Aggregation  | StarRocks real-time processing + `event_date` partitioning + multi-table joins |
| Modeling     | Bayesian inference + uplift + significance testing                             |
| Scheduling   | Supports CLI, Airflow, DolphinScheduler, Crontab                               |
| Data Service | Standardized schema; auto-integrated with BI tools like Metabase               |

---

## 🕐 Usage Examples

```bash
# Run analysis for a single tag (recommended for debugging)
python main_run.py --tag=retention

# Run full analysis across all business modules
python main_all.py
```

---

## 🔁 FAQ

### Q1: Is a tag the same as an experiment?

No. A tag represents a **business domain** (e.g., retention, chat\_entry), not a single experiment. The system will pull all related experiments under this tag and use the **latest phase only** for analysis.

### Q2: Will each run generate multiple results?

No. The output for each tag is **overwritten** on every run, ensuring only the **latest experiment results** are available in downstream dashboards.

### Q3: Does it support daily runs?

Yes. All wide and report tables are partitioned by `event_date`, supporting both full and incremental runs.




# 📊 A/B 测试全链路数据平台（GrowthBook + StarRocks）

本项目构建了一套企业级、模块化的 A/B 测试分析平台，围绕 GrowthBook 实验配置和 StarRocks 数仓进行全链路开发，实现从实验配置拉取、用户行为聚合、建模分析，到自动化写入报表和 Metabase 可视化看板的完整闭环。支持广告、订阅、留存、聊天等多业务线并行运行与统一评估。

---

## 1️⃣ 实验配置拉取与元信息入库

* 系统接入 **GrowthBook API**，自动拉取实验配置，包括实验名、variation 分组、流量参数、指标定义等；
* 所有实验配置和用户分流信息统一写入 StarRocks 中的 `experiment_data` 表；
* 平台使用 `tag` 作为核心调度标识，代表一个**业务模块或团队下的实验集群**，不是单次实验或版本号。

### 🧠 `tag` 的准确定义：

* 每个 `tag` 通常绑定一个**业务部门/场景维度**（如 `retention`、`subscribe`、`chat_entry` 等）；
* 系统根据 tag 自动获取该业务下的所有相关实验，\*\*并自动选取其中“最近一次 phase”的实验配置（起止时间、variation 结构）\*\*作为分析依据；
* 平台支持多实验共存，但分析结果始终使用最新配置并**覆盖写入相应报表**，确保每个 tag 仅呈现最新实验结论，表名复用可以自动在BI metabase 上覆盖数据结果。

---

## 2️⃣ 用户行为宽表构建（基于 tag + event\_date 动态生成）

* 基于 StarRocks 实时引擎，汇总用户在实验周期内的关键行为（如点击、活跃、订阅、支付等）；
* 系统根据 `tag` 和 `event_date` 自动构建命名规则一致的宽表（如 `tbl_wide_user_retention_retention`）；
* 宽表字段统一标准化，支持跨模块、跨团队复用与版本控制；
* 支持每日增量执行和完整实验周期回溯，便于调度、测试与业务解释。

---

## 3️⃣ 实验建模与评估逻辑

* 平台支持多种实验分析方法，包括：

  * ✅ 贝叶斯推断（胜率估计 / 均值后验）
  * ✅ Uplift 模型（净提升率评估）
  * ✅ t 检验与置信区间计算
* 分析结果输出至以 `tag` 命名的标准报表表（如 `tbl_report_user_retention_<tag>`），结构已对齐 Metabase 看板；
* 每次运行会使用最新配置**自动覆盖旧数据**，确保数据看板实时、准确、唯一。

---

## 4️⃣ 模块化架构与任务解耦

* 每个业务模块作为独立目录组织（如 Retention、Subscribe、Recharge 等），实现 ETL → 聚合 → 分析的闭环；
* 主程序通过 `main_run.py --tag retention` 可调度任意 tag 模块运行；
* 所有模块通过 tag 绑定数据源、实验配置与输出结构，彼此间完全解耦；
* 支持按需运行单模块、调度所有模块、或集成至调度平台。

---

## 📁 项目结构示意

```
state3/
├── Advertisement/         # 广告实验模块
├── Retention/             # 留存分析模块
├── Subscribe/             # 订阅实验模块
├── Recharge/              # 充值转化模块
├── Engagement/            # 聊天参与度实验
├── growthbook_fetcher/    # 实验配置拉取接口封装
├── main_all.py            # 多 tag 全量任务执行入口
├── main_run.py            # 单 tag 调试与测试脚本
└── README.md
```

---

## 💾 输出表说明（动态按 tag 命名）

| 表名                                             | 描述                             |
| ---------------------------------------------- | ------------------------------ |
| `experiment_data`                              | 存储 GrowthBook 实验配置与用户分组信息      |
| `tbl_wide_user_<业务模块>_<tag>`                   | 用户行为宽表，汇总点击、留存、订阅等关键指标         |
| `tbl_report_user_<业务模块>_<tag>`                 | 分析结果表，包含显著性、Uplift、贝叶斯胜率等      |
| `flow_report_app.tbl_report_ab_testing_result` | Metabase 报表用聚合展示表，结果按 tag 覆盖更新 |

---

## 🎯 平台业务价值

* 📌 全链路自动化，无需手工 SQL / notebook，提升实验分析效率 5 倍；
* 📌 多业务线共用平台结构，支持实验协作开发、统一评估逻辑；
* 📌 分析结果自动写回 Metabase 数据表，业务可直连看板实时查看；
* 📌 tag 解耦机制支持稳定扩展，便于团队持续接入新业务实验。

---

## 🧠 技术亮点

| 能力   | 技术说明                                   |
| ---- | -------------------------------------- |
| 数据接入 | GrowthBook API 实验配置拉取、自动分组解析           |
| 聚合引擎 | StarRocks 实时数仓 + event\_date 分区 + 多表关联 |
| 模型方法 | 贝叶斯 + Uplift + 显著性评估                   |
| 任务管理 | 支持 CLI、Airflow、DolphinScheduler 等多调度框架 |
| 数据服务 | 报表表结构标准化，自动支持 Metabase 看板展示            |

---

## 🕐 使用示例

```bash
# 单个 tag 分析（推荐用于测试/调试）
python main_run.py --tag=retention

# 执行全量多模块任务
python main_all.py
```

---

## 🔁 FAQ

### Q1：一个 tag 是不是一个实验？

不是。一个 tag 表示一个业务模块或团队（如 retention、chat\_entry），系统会基于该 tag 自动检索多个实验，并使用最近的 phase 分析。

### Q2：每次跑分析会生成多条记录吗？

不会。分析结果根据 tag 自动覆盖写入，保持报表中只存在**最新一轮实验结果**。

### Q3：是否支持按天跑？

是的。所有表结构均支持 `event_date` 分区，宽表和报告表可按需跑全量或增量。
