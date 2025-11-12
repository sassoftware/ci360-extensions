cd "$(dirname "$0")"
cd ..
set -x
docker compose --file docker-compose.db.yml up --detach --remove-orphans --build
