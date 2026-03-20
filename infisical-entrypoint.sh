#!/bin/sh
# Infisical secret fetcher for Helix
# Authenticates, exports secrets to /secrets/helix.env, polls every 5 minutes

DOMAIN="http://72.60.225.81:8095"
PROJECT_ID="6f93fc3f-a6f8-4559-afd2-cea14fe41448"
ENV="prod"
OUT="/secrets/helix.env"
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

    infisical export \
        --projectId="$PROJECT_ID" \
        --env="$ENV" \
        --path="/helix/sensitive" \
        --format=dotenv \
        --token="$TOKEN" \
        --domain="$DOMAIN" \
        --silent > "$OUT.tmp" 2>/dev/null

    if [ $? -eq 0 ] && [ -s "$OUT.tmp" ]; then
        mv "$OUT.tmp" "$OUT"
        echo "[infisical-agent] secrets written to $OUT ($(wc -l < $OUT) keys)"
    else
        echo "[infisical-agent] ERROR: export failed or empty" >&2
        rm -f "$OUT.tmp"
        return 1
    fi
}

echo "[infisical-agent] starting..."
fetch_secrets

while true; do
    sleep 300
    echo "[infisical-agent] refreshing secrets..."
    fetch_secrets
done
