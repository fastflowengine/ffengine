# FFEngine Community

Fast Flow Engine Community is the public core of the FFEngine product family.
It provides the community runtime, shared contracts, and the foundation that
the private Enterprise extension builds on.

## Repository Role

This repository is intended to become the public `ffengine/ffengine` repository.

- Public scope: Community code and shared contracts
- Private counterpart: `ffengine-enterprise`
- Public docs: `ffengine-docs`
- Private website: `ffengine-www`

## Installation

```bash
pip install -e .
```

## Governance

- Repository ownership model: GitHub organization, not a shared user account
- Required branch: `main`
- Expected protections: PR review, status checks, no direct push to `main`

See [docs/github-organization-runbook.md](/c:/fast-flow/FFEngineCommunity/docs/github-organization-runbook.md)
for the organization bootstrap and migration steps.

## Development & Test Environment (Docker)

To avoid local port conflicts, the FFEngine development environment allocates isolated ports for the testing cluster. The `.env` file must be present at the root directory to store database credentials.

**Active Port Mappings:**
- **Airflow Web UI (`core-airflow-webserver`):** `http://localhost:8085`
- **Airflow Postgres (`core-postgres`):** `localhost:5436`
- **Test Postgres (`test-postgres`):** `localhost:5435`
- **Test MSSQL (`test-mssql`):** `localhost:1433`
- **Test Oracle (`test-oracle`):** `localhost:1521`

**Start the Core Cluster (Airflow):**
```bash
docker-compose -p ffengine-core -f docker/docker-compose.yml --env-file .env up -d --remove-orphans
```

**Start the Test Databases (Isolated):**
```bash
docker-compose -p ffengine-test -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```
