#!/bin/bash
# Helix Phase 1 - Quick Deploy & Migrate
# Run this script to deploy Helix and migrate data from Forge

set -e  # Exit on error

echo "🧬 Helix Phase 1 - Deploy & Migrate"
echo "===================================="
echo ""

# Step 1: Deploy Helix
echo "📦 Step 1: Deploying Helix container..."
cd /opt/projects/helix
docker-compose up -d --build

# Wait for startup
echo "⏳ Waiting 30 seconds for container to start..."
sleep 30

# Step 2: Check health
echo ""
echo "🏥 Step 2: Checking health..."
curl -s http://localhost:9050/health | jq '.'
echo ""
curl -s http://localhost:9050/ready | jq '.'

# Step 3: Check if databases exist
echo ""
echo "🔍 Step 3: Checking databases..."
if [ ! -f "/opt/projects/the-forge/data/forge.db" ]; then
    echo "❌ Forge database not found at /opt/projects/the-forge/data/forge.db"
    echo "Cannot migrate. Helix is deployed but empty."
    exit 1
fi

echo "✓ Found Forge database"
echo "✓ Found Helix database (created on startup)"

# Step 4: Run migration
echo ""
echo "📊 Step 4: Migrating data from Forge to Helix..."
python3 /opt/projects/helix/migrate.py

# Step 5: Verify
echo ""
echo "✅ Step 5: Verifying migration..."
echo ""
echo "Atom count:"
docker exec helix-cortex sqlite3 /app/data/cortex.db "SELECT COUNT(*) as total_atoms FROM atoms;"

echo ""
echo "Atoms with metadata:"
docker exec helix-cortex sqlite3 /app/data/cortex.db "SELECT COUNT(*) as atoms_with_meta FROM atoms WHERE meta != '{}';"

echo ""
echo "Sample atoms:"
docker exec helix-cortex sqlite3 /app/data/cortex.db \
    "SELECT id, name, substr(meta, 1, 80) as meta_preview FROM atoms WHERE meta != '{}' LIMIT 3;"

echo ""
echo "🎉 Deployment Complete!"
echo ""
echo "Helix is now running on:"
echo "  - http://localhost:9050"
echo "  - http://helix.millyweb.com (via Traefik)"
echo ""
echo "API Documentation:"
echo "  - http://localhost:9050/docs"
echo ""
echo "Next: Update Forge to write new atoms to Helix"
