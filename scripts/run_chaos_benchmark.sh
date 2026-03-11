#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ITERATIONS="${1:-100}"
SLF4J_JAR="$(find "${HOME}/.m2/repository/org/slf4j/slf4j-api" -name 'slf4j-api-*.jar' | sort | tail -1)"

mvn -q -pl consistency-test -am -DskipTests compile

if [[ -z "${SLF4J_JAR}" ]]; then
  echo "slf4j-api jar not found in local Maven repository" >&2
  exit 1
fi

CLASSPATH="${ROOT_DIR}/consistency-core/target/classes:${ROOT_DIR}/consistency-test/target/classes:${SLF4J_JAR}"

java -cp "${CLASSPATH}" \
  io.github.cacheconsistency.testkit.ChaosScenarioBenchmark \
  "--iterations=${ITERATIONS}"
