set -x
cd "$(dirname "$0")"
cd ..

docker compose --file docker-compose.api.yml down --remove-orphans 
docker compose --file docker-compose.db.yml down --remove-orphans
 
