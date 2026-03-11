# Example Spring Boot MySQL Redis

这个模块提供一个可以直接启动的 Spring Boot demo，用来演示：

- `ConsistencyClient<UserProfile>` 的业务接入
- MySQL 版本校验与事务实现
- `BatchPersistentOperation<UserProfile>` 的批量读/写/删实现
- Redis 协议执行和 `ProtocolInspector`
- 幽灵写治疗接口 `VersionHealingOperation`
- 可选的 finalize 失败补偿扩展
- Micrometer 指标和 Actuator 暴露

## 启动依赖

```bash
docker compose -f example-spring-boot-mysql-redis/compose.yaml up -d
```

## 启动应用

```bash
mvn -pl example-spring-boot-mysql-redis -am spring-boot:run
```

也可以通过环境变量覆盖默认本地配置：

```bash
export DEMO_MYSQL_URL="jdbc:mysql://127.0.0.1:3306/cache_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
export DEMO_MYSQL_USERNAME="demo"
export DEMO_MYSQL_PASSWORD="demo"
export DEMO_REDIS_URL="redis://127.0.0.1:6379"
```

如果你想在 demo 里打开“finalize 失败后补偿重试”，可以再加：

```bash
export DEMO_COMPENSATION_ENABLED=true
export DEMO_COMPENSATION_MAX_RETRIES=3
export DEMO_COMPENSATION_RETRY_DELAY_MILLIS=500
export DEMO_COMPENSATION_STORE_PATH=runtime/cck-compensation.log
```

## 示例接口

```bash
curl http://127.0.0.1:8080/api/users/user:1001

curl -X PUT http://127.0.0.1:8080/api/users/user:1001 \
  -H 'Content-Type: application/json' \
  -d '{"name":"alice v2","email":"alice.v2@example.com","expectedVersion":"1"}'

curl "http://127.0.0.1:8080/api/users?ids=user:1001,user:1002"

curl -X PUT http://127.0.0.1:8080/api/users \
  -H 'Content-Type: application/json' \
  -d '[{"userId":"user:1001","name":"alice-b1","email":"alice-b1@example.com","expectedVersion":"1"},{"userId":"user:1002","name":"bob-b1","email":"bob-b1@example.com","expectedVersion":"1"}]'

curl -X DELETE http://127.0.0.1:8080/api/users \
  -H 'Content-Type: application/json' \
  -d '[{"userId":"user:1001","expectedVersion":"2"},{"userId":"user:1002","expectedVersion":"2"}]'

curl http://127.0.0.1:8080/api/protocol/user:1001

curl http://127.0.0.1:8080/api/protocol/user:1001/diagnosis

curl http://127.0.0.1:8080/api/protocol/user:1001/read-debug

curl http://127.0.0.1:8080/api/protocol/user:1001/write-debug

curl "http://127.0.0.1:8080/api/protocol/user:1001/write-debug/history?limit=5"

curl "http://127.0.0.1:8080/api/protocol?keys=user:1001,user:1002"

curl "http://127.0.0.1:8080/api/protocol/diagnosis?keys=user:1001,user:1002"

curl "http://127.0.0.1:8080/api/protocol/read-debug?keys=user:1001,user:1002"

curl "http://127.0.0.1:8080/api/protocol/write-debug?keys=user:1001,user:1002"

curl "http://127.0.0.1:8080/api/protocol/write-debug/history?keys=user:1001,user:1002&limit=5"
```

## 可选补偿说明

demo 默认不会启用 compensation。只有当 `DEMO_COMPENSATION_ENABLED=true` 时，配置类才会额外注册：

- `AsyncCompensationExecutor`
- `CompensationFinalizeFailureHandler`

这样 `ConsistencyClient` 才会在 Redis `finalizeWrite()` 失败后，把缓存修复动作落到异步补偿队列。

补偿任务会持久化到 `DEMO_COMPENSATION_STORE_PATH` 指定的文件，并在应用启动时自动 `replayPending()`。

batch 写删接口支持每个 item 提供独立的 `expectedVersion`，底层会映射到 `setAllWithContexts(...)` / `deleteAllWithContexts(...)` 的 per-item context。

batch 写删响应里还会带一个 `debug` 对象，展示每个 key 的 `prepare/version/store/stage/finalize` 步骤结果，以及是否进入了补偿分支。

demo 还会在内存里保留每个 key 最近 10 条 batch write/delete debug 记录，可以通过 `write-debug/history` 接口单独查询；它不会落库，进程重启后会清空。
