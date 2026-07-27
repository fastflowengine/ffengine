# FFEngine Community Installation

This document tracks the work required to publish and install the public
FFEngine Community Docker image. The runtime image, Compose file, and
environment template must be released together.

## Public Distribution

- Docker image: `caglarsahin/ffengine:0.1.0-airflow3.2.2`
- Compose file: [`docker/docker-compose.hub.yml`](docker/docker-compose.hub.yml)
- Website: https://www.ffengine.com
- Contact: contact@ffengine.com
- License: [Apache License 2.0](LICENSE)

Do not use the `latest` tag until the versioned image has passed all release
checks. Production and repeatable test deployments should use a versioned tag.

## Publisher Checklist

### Security

- [x] Remove default passwords from `docker/seed_airflow_users.py`.
- [x] Make user seeding fail when a required password variable is missing.
- [x] Remove password and secret fallback values from the public Compose file.
- [x] Keep `.env`, keys, tokens, database dumps, logs, checkpoints, generated
      DAGs, and project runtime data outside the image.
- [x] Verify `.dockerignore` excludes private and generated content.
- [x] Set `AIRFLOW__CORE__AUTH_MANAGER` in the image itself so Docker Hub
      runtime cannot fall back to Airflow simple/default auth.
- [ ] Verify `docker history --no-trunc` contains no credentials.
- [ ] Scan the final image with Docker Scout, Trivy, or an equivalent scanner.

### Runtime Image

- [x] Install FFEngine as a normal package, not with editable mode (`-e`).
- [x] Exclude development dependencies such as pytest, black, flake8, and
      the remote debug client from the public runtime image.
- [x] Include Flow Studio HTML, CSS, and JavaScript as Python package data.
- [x] Use the Airflow 3.2.2 constraints bundled with the base image.
- [x] Run the final container as the `airflow` user.
- [x] Keep compiler and development packages out of the final image layer.
- [ ] Document whether a Microsoft ODBC driver is included or must be supplied
      by users.

### Installation Bundle

- [x] Publish a Compose file that uses the versioned public image.
- [x] Include API server, scheduler, DAG processor, init, and PostgreSQL
      services.
- [x] Publish a secret-free `.env.example` next to the Compose file.
- [x] Do not package `.env.example` inside the runtime image; distribute it in
      the source repository or release bundle.
- [x] Replace the current test-only `.env.example` with, or supplement it by,
      a public runtime template containing the required variables below.

Required public runtime variables:

```dotenv
POSTGRES_PASSWORD=
AIRFLOW_FERNET_KEY=
AIRFLOW_API_SECRET_KEY=
AIRFLOW_API_AUTH_JWT_SECRET=
FFENGINE_AIRFLOW_ADMIN_PASSWORD=
FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD=
FFENGINE_AIRFLOW_OP_PASSWORD=
FFENGINE_AIRFLOW_VIEWER_PASSWORD=
```

Database test variables may remain in a separate `.env.test.example` file.

### Release Verification

- [ ] Build `caglarsahin/ffengine:0.1.0-airflow3.2.2`.
- [ ] Verify `airflow version` returns `3.2.2`.
- [ ] Run `pip check` inside the image.
- [ ] Verify `import ffengine` and `import ffengine.ui.plugin`.
- [ ] Verify image metadata contains
      `AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`.
- [ ] Verify running Airflow resolves `core.auth_manager` to `FabAuthManager`.
- [ ] Verify seeded users exist in the FAB `ab_user` table.
- [ ] Verify Flow Studio templates and static assets load from the installed
      package.
- [ ] Run unit tests, auth integration tests, DAG import checks, and the dummy
      DAG smoke test.
- [ ] Push the versioned tag to Docker Hub.
- [ ] Pull the published image into a clean environment and repeat smoke tests.
- [ ] Verify the image can also be pulled through the corporate Nexus proxy.
- [ ] Assign `latest` only after all checks pass.

## User Installation

The public image does not contain an `.env` file. Obtain the Compose file and
`.env.example` from the FFEngine source repository or matching release bundle.

1. Copy the environment template:

   ```bash
   cp .env.example .env
   ```

2. Generate unique values for every secret and password in `.env`. Do not use
   example or shared credentials.

3. Keep `.env` local. Do not commit it or copy it into a Docker image.

4. Create controlled runtime directories:

   ```bash
   mkdir -p runtime/dags runtime/projects
   ```

   On Linux hosts, make these directories writable by the Airflow container
   user:

   ```bash
   sudo chown -R 50000:0 runtime
   ```

5. Validate the resolved Compose configuration:

   ```bash
   docker compose -f docker/docker-compose.hub.yml --env-file .env config
   ```

6. Pull and start the versioned release:

   ```bash
   docker compose -f docker/docker-compose.hub.yml --env-file .env pull
   docker compose -f docker/docker-compose.hub.yml --env-file .env up -d
   ```

7. Verify the installation:

   ```bash
   docker compose -f docker/docker-compose.hub.yml ps
   docker exec ffengine-api-server airflow version
   docker exec ffengine-scheduler airflow dags list-import-errors
   ```

Expected results are Airflow `3.2.2`, healthy services, and no DAG import
errors.

## Nexus-Based Pull

Networks that proxy Docker Hub through Nexus should keep using the same image
tag through the Nexus registry path:

```text
nexus.paycore.com/docker.io/caglarsahin/ffengine:0.1.0-airflow3.2.2
```

The Docker Hub and Nexus-proxied image digests should match.
