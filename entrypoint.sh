#!/bin/sh
# entrypoint.sh — Fix DATABASE_URL scheme for asyncpg before starting the app.
# Render's managed Postgres provides `postgresql://` but asyncpg requires `postgresql+asyncpg://`.

set -e

# Convert scheme if needed (only when the +asyncpg suffix is absent)
if echo "${DATABASE_URL}" | grep -q "^postgresql://"; then
    export DATABASE_URL="postgresql+asyncpg://${DATABASE_URL#postgresql://}"
fi

# Also export CELERY_BROKER_URL / CELERY_RESULT_BACKEND from REDIS_URL when not set separately
if [ -z "${CELERY_BROKER_URL}" ] && [ -n "${REDIS_URL}" ]; then
    export CELERY_BROKER_URL="${REDIS_URL}"
fi
if [ -z "${CELERY_RESULT_BACKEND}" ] && [ -n "${REDIS_URL}" ]; then
    export CELERY_RESULT_BACKEND="${REDIS_URL}"
fi

exec "$@"
