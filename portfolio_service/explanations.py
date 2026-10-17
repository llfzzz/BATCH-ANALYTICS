"""Deterministic evidence; no network, LLM calls, invented skills or eligibility promises."""
def local_explanation(item_id, report):
    item = next((row for row in report.get("matches", []) if row["job_id"] == item_id), None)
    if item is None:
        raise KeyError(item_id)
    show = lambda values: "/".join(values) or "未标注"
    return {
        "mode": "local_evidence",
        "summary": "通过本次结构化硬条件筛选。文本相似度只决定排序，不能证明具备录用资格。",
        "evidence": [f"岗位：{item['title']}；来源：{item.get('source', '未标注')}。",
                     f"距公开筛选中心 {item['distance_km']:.3f} 公里，直线距离。",
                     f"已有技能交集：{show(item.get('matched_skills', []))}。",
                     f"显式必要技能：{show(item.get('required_skills', []))}。",
                     f"偏好技能：{show(item.get('preferred_skills', []))}。",
                     f"未满足偏好技能：{show(item.get('missing_preferred_skills', []))}。",
                     f"未分级技能缺口：{show(item.get('missing_skills', []))}。",
                     f"TF-IDF 文本相似度：{item['score']:.4f}。"],
        "limitations": ["默认岗位和公司完全虚构，只用于项目演示。", "只有显式 required_skills 参与技能硬过滤，未从自由文本推断。", "画像不验证熟练度或经历，最终岗位要求需人工确认。"]}
