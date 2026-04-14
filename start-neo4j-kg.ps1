$ErrorActionPreference = "Stop"

if (-not $env:NEO4J_PASSWORD -or [string]::IsNullOrWhiteSpace($env:NEO4J_PASSWORD)) {
    $env:NEO4J_PASSWORD = "neo4j123"
}

docker compose -f docker-compose.neo4j.yml up --build
