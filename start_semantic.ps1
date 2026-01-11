# ═══════════════════════════════════════════════════════════════════════════════
# FAME Data Space - Semantic Integration Startup (PowerShell)
# ═══════════════════════════════════════════════════════════════════════════════
#
# This script:
# 1. Starts Docker services (including Fuseki)
# 2. Waits for Fuseki to be ready
# 3. Loads Protégé ontology files into named graphs
# 4. Validates the loaded data
#
# Usage:
#   .\start_semantic.ps1              # Full startup
#   .\start_semantic.ps1 -LoadOnly    # Only load data (assumes Docker is running)
#   .\start_semantic.ps1 -ValidateOnly # Only validate existing data
# ═══════════════════════════════════════════════════════════════════════════════

param(
    [switch]$LoadOnly,
    [switch]$ValidateOnly
)

# Configuration
$FUSEKI_HOST = if ($env:FUSEKI_HOST) { $env:FUSEKI_HOST } else { "localhost" }
$FUSEKI_PORT = if ($env:FUSEKI_PORT) { $env:FUSEKI_PORT } else { "3030" }
$FUSEKI_DATASET = if ($env:FUSEKI_DATASET) { $env:FUSEKI_DATASET } else { "fame" }
$FUSEKI_USER = if ($env:FUSEKI_USER) { $env:FUSEKI_USER } else { "admin" }
$FUSEKI_PASSWORD = if ($env:FUSEKI_PASSWORD) { $env:FUSEKI_PASSWORD } else { "admin123" }

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SemanticDir = Join-Path $ScriptDir "semantic"

# Header
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   🚀 FAME Data Space - Semantic Layer Integration" -ForegroundColor Cyan
Write-Host "   📚 Protégé + Apache Fuseki" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Function to wait for Fuseki
function Wait-ForFuseki {
    Write-Host "⏳ Waiting for Fuseki to be ready..." -ForegroundColor Yellow
    
    $maxAttempts = 30
    $attempt = 1
    
    while ($attempt -le $maxAttempts) {
        try {
            $response = Invoke-WebRequest -Uri "http://${FUSEKI_HOST}:${FUSEKI_PORT}/`$/ping" -Method Get -TimeoutSec 5 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Host "✅ Fuseki is ready!" -ForegroundColor Green
                return $true
            }
        }
        catch {
            # Ignore errors, keep trying
        }
        
        Write-Host "   Attempt $attempt/$maxAttempts..."
        Start-Sleep -Seconds 2
        $attempt++
    }
    
    Write-Host "❌ Fuseki did not become ready in time" -ForegroundColor Red
    return $false
}

# Function to load RDF file into named graph
function Load-RDFFile {
    param(
        [string]$FilePath,
        [string]$GraphUri,
        [string]$ContentType
    )
    
    if (-not (Test-Path $FilePath)) {
        Write-Host "   ⚠️  File not found: $FilePath" -ForegroundColor Yellow
        return $false
    }
    
    $filename = Split-Path -Leaf $FilePath
    Write-Host "   📤 Loading: $filename → $GraphUri" -ForegroundColor Blue
    
    try {
        $credentials = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${FUSEKI_USER}:${FUSEKI_PASSWORD}"))
        
        $headers = @{
            "Authorization" = "Basic $credentials"
            "Content-Type" = $ContentType
        }
        
        $content = [System.IO.File]::ReadAllBytes($FilePath)
        
        $uri = "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/data?graph=$GraphUri"
        
        $response = Invoke-WebRequest -Uri $uri -Method Put -Headers $headers -Body $content -TimeoutSec 60
        
        if ($response.StatusCode -in @(200, 201, 204)) {
            Write-Host "   ✅ Loaded successfully" -ForegroundColor Green
            return $true
        }
        else {
            Write-Host "   ❌ Failed with HTTP $($response.StatusCode)" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "   ❌ Error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Function to count triples in graph
function Get-TripleCount {
    param([string]$GraphUri)
    
    try {
        $query = "SELECT (COUNT(*) as ?count) WHERE { GRAPH <$GraphUri> { ?s ?p ?o } }"
        
        $credentials = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${FUSEKI_USER}:${FUSEKI_PASSWORD}"))
        
        $headers = @{
            "Authorization" = "Basic $credentials"
            "Accept" = "application/sparql-results+json"
            "Content-Type" = "application/x-www-form-urlencoded"
        }
        
        $body = "query=$([System.Web.HttpUtility]::UrlEncode($query))"
        
        $response = Invoke-WebRequest -Uri "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/query" `
            -Method Post -Headers $headers -Body $body -TimeoutSec 30
        
        $result = $response.Content | ConvertFrom-Json
        return [int]$result.results.bindings[0].count.value
    }
    catch {
        return 0
    }
}

# Function to validate loaded data
function Test-LoadedData {
    Write-Host "`n🔍 Validating loaded data..." -ForegroundColor Cyan
    
    $graphs = @(
        "http://fame.eu/graph/ontology",
        "http://fame.eu/graph/data",
        "http://fame.eu/graph/vocabulary"
    )
    
    $total = 0
    
    foreach ($graph in $graphs) {
        $count = Get-TripleCount -GraphUri $graph
        $total += $count
        $graphName = $graph.Split("/")[-1]
        Write-Host "   📊 ${graphName}: " -NoNewline
        Write-Host "$count" -ForegroundColor Green -NoNewline
        Write-Host " triples"
    }
    
    Write-Host "`n   📈 Total triples: " -NoNewline
    Write-Host "$total" -ForegroundColor Green
    
    # Test query for stocks
    Write-Host "`n🧪 Testing sample query..." -ForegroundColor Cyan
    
    try {
        $testQuery = "PREFIX fame: <http://fame.eu/ontology#> SELECT (COUNT(?stock) as ?count) WHERE { ?stock a fame:Stock }"
        
        $credentials = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${FUSEKI_USER}:${FUSEKI_PASSWORD}"))
        
        $headers = @{
            "Authorization" = "Basic $credentials"
            "Accept" = "application/sparql-results+json"
            "Content-Type" = "application/x-www-form-urlencoded"
        }
        
        $body = "query=$([System.Web.HttpUtility]::UrlEncode($testQuery))"
        
        $response = Invoke-WebRequest -Uri "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/query" `
            -Method Post -Headers $headers -Body $body -TimeoutSec 30
        
        $result = $response.Content | ConvertFrom-Json
        $stockCount = $result.results.bindings[0].count.value
        
        Write-Host "   🏢 Stocks found: " -NoNewline
        Write-Host "$stockCount" -ForegroundColor Green
    }
    catch {
        Write-Host "   ⚠️  Query test failed" -ForegroundColor Yellow
    }
}

# Main execution
function Main {
    # Start Docker if not LoadOnly or ValidateOnly
    if (-not $LoadOnly -and -not $ValidateOnly) {
        Write-Host "`n🐳 Starting Docker services..." -ForegroundColor Cyan
        docker-compose up -d fuseki
        Start-Sleep -Seconds 5
    }
    
    # Wait for Fuseki
    if (-not $ValidateOnly) {
        $ready = Wait-ForFuseki
        if (-not $ready) {
            Write-Host "Exiting due to Fuseki not being ready" -ForegroundColor Red
            exit 1
        }
    }
    
    # Load data if not ValidateOnly
    if (-not $ValidateOnly) {
        Write-Host "`n📂 Loading Protégé files into Fuseki..." -ForegroundColor Cyan
        Write-Host "────────────────────────────────────────────────────────────"
        
        # Load Ontology (OWL)
        Write-Host "`n📦 Loading ONTOLOGY (TBox)..." -ForegroundColor Yellow
        Load-RDFFile -FilePath (Join-Path $SemanticDir "fame_data_protege.owl") `
                     -GraphUri "http://fame.eu/graph/ontology" `
                     -ContentType "application/rdf+xml"
        Load-RDFFile -FilePath (Join-Path $SemanticDir "fame_ontology.owl") `
                     -GraphUri "http://fame.eu/graph/ontology" `
                     -ContentType "application/rdf+xml"
        
        # Load Instance Data (RDF)
        Write-Host "`n📝 Loading INSTANCE DATA (ABox)..." -ForegroundColor Yellow
        Load-RDFFile -FilePath (Join-Path $SemanticDir "FAME-RDF.rdf") `
                     -GraphUri "http://fame.eu/graph/data" `
                     -ContentType "application/rdf+xml"
        
        # Load Vocabulary (SKOS)
        Write-Host "`n📖 Loading VOCABULARY (SKOS)..." -ForegroundColor Yellow
        Load-RDFFile -FilePath (Join-Path $SemanticDir "FAME-SKOS.ttl") `
                     -GraphUri "http://fame.eu/graph/vocabulary" `
                     -ContentType "text/turtle"
        Load-RDFFile -FilePath (Join-Path $SemanticDir "fame_vocabulary.skos") `
                     -GraphUri "http://fame.eu/graph/vocabulary" `
                     -ContentType "text/turtle"
    }
    
    # Validate
    Test-LoadedData
    
    # Summary
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "✅ FAME Semantic Layer Integration Complete!" -ForegroundColor Green
    Write-Host "═══════════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   🌐 Fuseki UI:     " -NoNewline
    Write-Host "http://localhost:3030" -ForegroundColor Blue
    Write-Host "   📊 SPARQL Query:  " -NoNewline
    Write-Host "http://localhost:3030/fame/query" -ForegroundColor Blue
    Write-Host "   📝 SPARQL Update: " -NoNewline
    Write-Host "http://localhost:3030/fame/update" -ForegroundColor Blue
    Write-Host ""
    Write-Host "   📚 Named Graphs:"
    Write-Host "      • Ontology:   http://fame.eu/graph/ontology"
    Write-Host "      • Data:       http://fame.eu/graph/data"
    Write-Host "      • Vocabulary: http://fame.eu/graph/vocabulary"
    Write-Host ""
}

# Load System.Web for URL encoding
Add-Type -AssemblyName System.Web

# Run main
Main
