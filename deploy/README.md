# 个人网站部署

线上入口：<https://woxiangchuanaj.top/batch-analytics/>，从个人主页 Work 进入。

当前服务器使用项目自带的**公开虚构样例**，运行真实只读 FastAPI 服务；不在此主机启动 Spark。页面与 `/api/jobs.presentation` 都标注 `example`，没有把历史样例称为本次计算结果。完整管道仍使用仓库原有的 `make run` / `make verify`。

在 `/var/www/BATCH-ANALYTICS` 下，使用 Python 3.12 安装轻量服务依赖：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-web.lock
sudo install -m 644 deploy/batch-analytics.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now batch-analytics
curl --fail http://127.0.0.1:8767/health
```

`requirements-web.lock` 从完整锁文件保留 Web 运行时依赖，不含 PySpark；完整计算或测试环境应按根 README 安装 `requirements.lock`。轻量环境检查 API 时可额外安装 `pytest==8.4.2` 后执行 `.venv/bin/python -m pytest service_tests`。

服务使用 `www-data`，只监听 `127.0.0.1:8767`，启用开机启动、异常重启、只读文件系统与 256 MB 内存上限。源码、虚拟环境与公开报告需对该用户可读；报告应由维护命令生成，Web 进程不写入数仓。

反向代理由 `personal-index/deploy/nginx/data-projects.conf` 统一维护。Nginx 去掉 `/batch-analytics/` 前缀后转发；Uvicorn `--root-path` 保持接口文档路径正确。静态文件与 API 使用相对地址，单独在 `/` 启动也可使用。无尾斜杠入口会跳转到带斜杠入口。

更新后：`sudo systemctl restart batch-analytics`。日志：`journalctl -u batch-analytics`。健康检查：`https://woxiangchuanaj.top/batch-analytics/health`。
