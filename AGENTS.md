# Service ownership

This repository owns only knowledge. Read README.md before changing the service.
Do not import integration source or sibling repositories. Keep fixed SDK versions and vendor in sync; no local replace directives.
SQL, route authorization, readiness and migrations belong here. Never read another service's database.
Use Go and bounded resources. Test with GOWORK=off; real PostgreSQL tests require TEST_DATABASE_URL and create isolated fixture databases.
Never commit real credentials, config, user data or model outputs. Do not claim skipped tests passed.
