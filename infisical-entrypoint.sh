#!/bin/sh
# Infisical secret fetcher for Helix + Content Pipeline
# Authenticates, exports secrets, polls every 5 minutes

DOMAIN="http://72.60.225.81:8095"
PROJECT_ID="6f93fc3f-a6f8-4559-afd2-cea14fe41448"
ENV="prod"
OUT_HELIX="/secrets/helix.env"
OUT_CONTENT="/secrets/content-pipeline.env"
CID_FILE="/secrets/client-id"
CS_FILE="/secrets/client-secret"

fetch_secrets() {
    TOKEN=$(infisical login \
        --method=universal-auth \
        --client-id="$(cat $CID_FILE)" \
        --client-secret="$(cat $CS_FILE)" \
        --domain="$DOMAIN" \
        --plain \
        --silent 2>/dev/null)

    if [ -z "$TOKEN" ]; then
        echo "[infisical-agent] ERROR: failed to get token" >&2
        return 1
    fi

    # Helix secrets (unchanged)
    infisical export \
        --projectId="$PROJECT_ID" \
        --env="$ENV" \
        --path="/helix/sensitive" \
        --format=dotenv \
        --token="$TOKEN" \
        --domain="$DOMAIN" \
        --silent > "$OUT_HELIX.tmp" 2>/dev/null

    if [ $? -eq 0 ] && [ -s "$OUT_HELIX.tmp" ]; then
        mv "$OUT_HELIX.tmp" "$OUT_HELIX"
        echo "[infisical-agent] helix secrets written ($(wc -l < $OUT_HELIX) keys)"
    else
        echo "[infisical-agent] ERROR: helix export failed" >&2
        rm -f "$OUT_HELIX.tmp"
    fi

    # Content pipeline secrets - ElevenLabs
    infisical export \
        --projectId="$PROJECT_ID" \
        --env="$ENV" \
        --path="/Elevenlabs" \
        --format=dotenv \
        --token="$TOKEN" \
        --domain="$DOMAIN" \
        --silent > "$OUT_CONTENT.tmp" 2>/dev/null

    # HeyGen secrets - append to same file
    infisical export \
        --projectId="$PROJECT_ID" \
        --env="$ENV" \
        --path="/Heygen" \
        --format=dotenv \
        --token="$TOKEN" \
        --domain="$DOMAIN" \
        --silent >> "$OUT_CONTENT.tmp" 2>/dev/null

    # Also grab ANTHROPIC_API_KEY from helix env
    grep 'ANTHROPIC_API_KEY' "$OUT_HELIX" >> "$OUT_CONTENT.tmp" 2>/dev/null

    if [ $? -eq 0 ] && [ -s "$OUT_CONTENT.tmp" ]; then
        mv "$OUT_CONTENT.tmp" "$OUT_CONTENT"
        echo "[infisical-agent] content pipeline secrets written ($(wc -l < $OUT_CONTENT) keys)"
    else
        echo "[infisical-agent] WARNING: content pipeline export empty or failed" >&2
        rm -f "$OUT_CONTENT.tmp"
    fi
}

echo "[infisical-agent] starting..."
fetch_secrets

while true; do
    sleep 300
    echo "[infisical-agent] refreshing secrets..."
    fetch_secrets
done
