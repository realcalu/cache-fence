# Contributing

## Workflow

- Fork the repository and create a topic branch.
- Keep changes focused; protocol changes and demo changes should be separated when practical.
- Add or update tests for behavioral changes.
- Run `mvn test` before opening a pull request.

## Pull Requests

- Explain the problem being solved and the protocol impact.
- Call out compatibility changes explicitly.
- Include benchmark or chaos results if you changed correctness-sensitive paths.

## Development Notes

- `consistency-core` contains the protocol and client logic.
- `consistency-spring` contains Spring and Boot integration.
- `consistency-test` contains in-memory fixtures, chaos simulations, and benchmarks.
- `example-spring-boot-mysql-redis` is a runnable demo, not the compatibility baseline.

