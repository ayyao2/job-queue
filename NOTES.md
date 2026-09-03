# Rebuild / Reset Database
docker compose exec -T db psql -U postgres -d jobs < schema.sql

# Enter Jobs Database
docker compose exec db psql -U postgres -d jobs