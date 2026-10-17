#!/usr/bin/env bash
# Source from an entry-point script. Supports installed Java 17/21 on macOS and Linux.
set -euo pipefail
if [[ -z "${JAVA_HOME:-}" ]] && [[ -x /usr/libexec/java_home ]]; then
  JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home -v 17 2>/dev/null || true)
  export JAVA_HOME
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if ! command -v java >/dev/null 2>&1; then
  echo '需要 Java 17 或 21，请安装 JDK 并设置 JAVA_HOME。' >&2; exit 1
fi
JAVA_MAJOR=$(java -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -n 1)
if [[ "$JAVA_MAJOR" != 17 && "$JAVA_MAJOR" != 21 ]]; then
  echo '本项目验证支持 Java 17/21；请设置 JAVA_HOME 指向相应 JDK。' >&2; exit 1
fi
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
