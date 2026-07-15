# Container deployment

Build the generated Gateway and World binaries into the default local image, configure unique
development secrets, and start both services:

```bash
docker build -t elura-game:dev .
cp .env.example .env
# Replace every placeholder in .env before continuing.
docker compose -f deploy/docker-compose.yml up
```

The Compose file mounts the non-secret JSON configuration from `config/`. Set `ELURA_IMAGE` in
`.env` when publishing or deploying a differently tagged image.
