# Security Policy

## Reporting

Do not open public issues for suspected security vulnerabilities.

Until a dedicated security contact is configured, report vulnerabilities privately to the project maintainers through the repository hosting platform's private reporting mechanism or direct maintainer contact.

## Scope

Security reports are especially useful for:

- Redis protocol races that can violate stated consistency guarantees
- Privilege or tenant isolation issues caused by key-prefix handling
- Unsafe demo defaults that could lead to accidental exposure
- Dependency issues with known exploitable impact

