# 数据字典与契约

## 输入 JSONL

一个 UTF-8 JSON 对象一行，文件名为不可变批次 ID。每条是**完整版本**，关闭版本也保留身份字段。

```json
{"source":"synthetic-export","source_job_id":"job-001","snapshot_ts":"2026-10-12T09:00:00+08:00","title":"数据开发实习生","role":"数据开发","company":"虚构·数据实验室","address":"虚构办公点001","city":"深圳","employment_type":"实习","education":"本科","graduation_year_min":2026,"graduation_year_max":2027,"salary_text":"200-300元/天","skills":["Python","SQL","Spark"],"required_skills":["SQL"],"preferred_skills":["Spark"],"description":"【虚构】参与ETL与质量开发","longitude":113.894,"latitude":22.560,"coordinate_system":"GCJ02","status":"active","synthetic":true,"source_url":null}
```

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| source/source_job_id | string | 非空，源内主键；unknown 来源保留为质量错误 |
| snapshot_ts | ISO timestamp | 可解析；决定版本顺序而非导入顺序 |
| title/role/company/address | string | 非空；公司/标题/地址/城市生成跨源近似业务指纹 |
| employment_type | string | 示例画像只接受实习；未知不通过 |
| education | string | 本科示例接受不限/大专/本科；未知不通过 |
| graduation_year_min/max | int | 显式包含画像年份；未知不通过，反向区间隔离 |
| salary_text | string/null | 支持日/月/年/小时、K/千/万/元、单值/区间；面议或未知格式保留未知，倒置或非正区间隔离 |
| skills | array[string] | 未分级技能标签，用于相似度和交集/差集，不自动变成硬要求 |
| required_skills | array[string]/null | 显式必要技能，缺失任一则排除；空表示未提供此类结构化要求，不代表岗位没有实际要求 |
| preferred_skills | array[string]/null | 显式偏好技能；不足仍可入选，输出偏好缺口 |
| description | string/null | 英文词与技能共同做TF-IDF；不会自动提取或推断硬要求 |
| longitude/latitude | number/null | 非法数值隔离；缺失保留但不匹配 |
| coordinate_system | string | 与筛选中心相同的GCJ02才计算距离 |
| status | string | active/closed；同时间关闭优先 |
| synthetic/source_url | bool/string | 样例标识和来源追溯，默认样例公司全部虚构 |

字段类型在 Spark 读取层按契约纠正。技能小写、去重、排序；PySpark/Spark SQL/Apache Spark统一为spark，Apache Kafka统一为kafka等。薪资月度比较按21.75天/月、8小时/天、12月/年，原文保留。

## 输出报告

顶层包括 `version/project/engine/generated_at/snapshot_id/summary/matches/skill_demand/salary_by_role/skill_trend/exclusion_summary/quality/match_policy/model/lineage/execution/empty_state`。

- `summary`：ODS原始数、有效去重历史、隔离、源内最新、跨源唯一、有效、关闭、硬过滤通过、批次、距离范围和薪资未知数。
- `matches`：最多50条，全部ADS结果仍在Parquet。包含岗位身份与来源、原始与折算薪资、精确计算后显示到三位的距离、0–1文本相似度、技能交集/差集，以及必要/偏好技能和各自缺口。
- `exclusion_summary`：`{reason,count}`，按最新唯一岗位统计；一条岗位可有多个原因，不能相加当作排除总数。
- 排除原因：`closed/employment_type/education/graduation_unknown/graduation_year/target_role/location_unknown_or_system/outside_radius/required_skills_missing`。
- `skill_trend`：`{as_of_date,skill,count,change}`，连续自然日as-of状态，技能消失时仍有0值；SQL LAG计算变化。
- `quality`：原因、最多6条原始例子、位置与薪资未知计数。
- `model.status`：trained/not_trained_empty_corpus，空语料路径为null，不引用旧模型。
- `execution`：cache_hit、derived_rows_rebuilt、fingerprint、new_batch_ids等。本次状态由API按snapshot_id与last-run.json合并，不修改不可变快照。
- `presentation.mode`：API添加 example/committed_snapshot，公开例子明确区分于本次运行结果。

## 版本与提交边界

源内最新按 `(source,source_job_id)` 的snapshot_ts取最大，再按job_id取跨源最新；时间相同优先closed，再做稳定排序。业务指纹为规范化公司/标题/地址/城市的SHA256前24位，不能代替真实统一岗位ID。

ODS按batch_id幂等提交。完整派生快照位于 `warehouse/snapshots/<fingerprint-prefix>-<random-id>/`，包含四层派生部分、模型和报告；`warehouse/current`原子指向成功快照。缓存校验每个已登记文件SHA，发现缺失或损坏则重建。ODS不会随派生失败回滚，但上一成功派生报告会保留。
