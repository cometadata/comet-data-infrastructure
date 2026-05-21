#!/usr/bin/env bash
set -e

# Build the DB connection string. Workers get no DB secrets, so this is skipped for them.
if [ -n "${DB_HOST:-}" ]; then
  export DB_PASSWORD_ENC=$(python -c "import urllib.parse,os; print(urllib.parse.quote(os.environ['DB_PASSWORD'], safe=''))")
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${DB_USER}:${DB_PASSWORD_ENC}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=require"

  # Save the DB conn so healthchecks and debug shells can source it.
  mkdir -p /opt/airflow
  cat > /opt/airflow/env.sh <<EOF
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
EOF
fi

# Dispatch by role
case "$1" in
  init)
    airflow db migrate
    airflow fab-db migrate          # FAB auth-manager tables (ab_*, session)
    # Rotate the fernet key when FERNET_KEY is "NEW,OLD".
    case "$AIRFLOW__CORE__FERNET_KEY" in
      *,*) airflow rotate-fernet-key ;;
    esac
    ;;
  api-server)
    # Create the admin user if it doesn't exist.
    airflow users create --username admin --role Admin \
      --password "$ADMIN_PASSWORD" --firstname Admin --lastname User \
      --email admin@example.com || true
    exec /entrypoint api-server
    ;;
  scheduler|dag-processor|triggerer)
    exec /entrypoint "$1"
    ;;
  *)
    # Worker: run the command from the ECS executor.
    exec /entrypoint "$@"
    ;;
esac
