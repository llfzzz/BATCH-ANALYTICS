"""Read-only recruitment API; committed snapshots and explicit public example fallback."""
import json
import math
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from . import __version__
from .explanations import local_explanation

ROOT = Path(__file__).resolve().parents[1]


def validate_report(report):
    if not isinstance(report, dict) or not isinstance(report.get("summary"), dict) or not isinstance(report.get("matches"), list):
        raise ValueError("summary/matches")
    for row in report["matches"]:
        if not isinstance(row, dict) or any(not isinstance(row.get(k), str) or not row[k] for k in ("job_id", "title")):
            raise ValueError("matches.job_id/title")
        for key in ("score", "distance_km"):
            number = row.get(key)
            if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
                raise ValueError("matches." + key)
        if row["score"] > 1:
            raise ValueError("matches.score")
        for key in ("matched_skills", "missing_skills"):
            if not isinstance(row.get(key), list) or any(not isinstance(x, str) for x in row[key]):
                raise ValueError("matches." + key)


class ExplainRequest(BaseModel):
    item_id: str = Field(min_length=1, max_length=256)


def create_app(data_root=ROOT, allow_examples=True):
    root = Path(data_root)
    app = FastAPI(title="招聘离线数仓与技能匹配", version=__version__)

    def read_report():
        path = root / "artifacts/report.json"
        example = False
        if not path.exists() and allow_examples:
            path = root / "examples/report.json"
            example = True
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            validate_report(report)
        except FileNotFoundError:
            raise HTTPException(404, "暂无数据，请运行 ./run-demo.sh，或保留 examples/report.json 查看明确标注的样例")
        except (ValueError, OSError, TypeError, KeyError) as error:
            raise HTTPException(503, f"报告不可读或契约错误，请重新运行管道：{type(error).__name__}")
        report["presentation"] = {"mode": "example" if example else "committed_snapshot", "label": "公开虚构样例，未在本次会话运行管道" if example else "本机已提交快照；来源与 synthetic 标识见报告"}
        if not example:
            try:
                latest = json.loads((root / "artifacts/last-run.json").read_text())
                if latest.get("snapshot_id") == report.get("snapshot_id"):
                    report["execution"].update(latest["execution"])
                    report["summary"].update(latest["summary"])
            except (OSError, ValueError, KeyError):
                pass
        return report

    @app.get("/health")
    def health():
        return {"status": "ok", "version": __version__}

    @app.get("/api/jobs")
    def jobs():
        return read_report()

    @app.get("/api/overview")
    def overview():
        report = read_report()
        return {"version": __version__, "summary": report["summary"], "presentation": report["presentation"], "execution": report.get("execution", {})}

    @app.post("/api/explain")
    def explain(request: ExplainRequest):
        try:
            return local_explanation(request.item_id, read_report())
        except KeyError:
            raise HTTPException(404, "该岗位不在本次导出中，请刷新列表")

    static = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(static / "index.html")
    return app


app = create_app()
