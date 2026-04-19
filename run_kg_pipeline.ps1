param(
    [string]$Neo4jPassword = "neo4j123",
    [switch]$Wipe,
    [switch]$SkipBuild,
    [switch]$SkipVerify
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Step([string]$Message) {
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Ensure-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

Step "Checking prerequisites"
Ensure-Command "python"
Ensure-Command "docker"

if (-not (Test-Path "augment_and_build_kg.py")) {
    throw "augment_and_build_kg.py not found. Run this script from the project root."
}

if (-not (Test-Path "load_kg_to_neo4j.py")) {
    throw "load_kg_to_neo4j.py not found. Run this script from the project root."
}

Step "Checking Docker engine"
docker info | Out-Null

if (-not $SkipBuild) {
    Step "Building KG tables and graph CSV"
    python augment_and_build_kg.py
}
else {
    Step "Skipping KG build step"
}

Step "Starting Neo4j container"
docker compose -f docker-compose.neo4j.yml up -d neo4j

Step "Importing KG into Neo4j"
$importArgs = @("load_kg_to_neo4j.py", "--password", $Neo4jPassword)
if ($Wipe) {
    $importArgs += "--wipe"
}
python @importArgs

if (-not $SkipVerify) {
    Step "Verifying imported graph counts"
    docker exec kg-neo4j cypher-shell -u neo4j -p $Neo4jPassword "MATCH (n) RETURN count(n) AS nodes;"
    docker exec kg-neo4j cypher-shell -u neo4j -p $Neo4jPassword "MATCH ()-[r]->() RETURN count(r) AS rels;"
}

Step "KG pipeline completed successfully"
