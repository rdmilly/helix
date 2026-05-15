"""Domain Classifier - writes domain meta namespace."""
import logging
from services.database import get_db
from services.meta import get_meta_service
log = logging.getLogger(__name__)
DOMAIN_RULES = {
    "infrastructure": {"keywords": ["docker","container","deploy","nginx","proxy","ssl","server","vps","compose","backup"], "name_patterns": ["deploy_","setup_","install_","configure_"]},
    "auth": {"keywords": ["auth","login","token","jwt","session","password","oauth","permission","credential","encrypt","verify"], "name_patterns": ["auth_","login_","verify_","check_token"]},
    "api": {"keywords": ["endpoint","route","request","response","http","rest","api","fastapi","handler","webhook","client"], "name_patterns": ["get_","post_","handle_","endpoint_"]},
    "data": {"keywords": ["database","postgres","sqlite","redis","query","insert","select","update","delete","schema","table","sql"], "name_patterns": ["save_","load_","fetch_","store_","query_"]},
    "automation": {"keywords": ["scrape","pipeline","batch","queue","worker","task","schedule","cron","job","process","workflow"], "name_patterns": ["run_","process_","batch_","queue_"]},
    "ai": {"keywords": ["llm","claude","gpt","prompt","embedding","vector","chromadb","model","inference","helix","atom","scanner"], "name_patterns": ["generate_","embed_","classify_","extract_","analyze_"]},
    "ui": {"keywords": ["render","template","html","css","react","component","page","view","form","display"], "name_patterns": ["render_","display_","show_"]},
}
def classify_domain(atom_name, semantic_tags, code=""):
    text = (atom_name + " " + " ".join(semantic_tags) + " " + code[:500]).lower()
    scores = {}
    for domain, rules in DOMAIN_RULES.items():
        score = sum(text.count(kw) for kw in rules["keywords"])
        score += sum(3 for pat in rules["name_patterns"] if atom_name.lower().startswith(pat))
        if score > 0: scores[domain] = score
    if not scores: return {"primary": "general", "categories": ["general"], "confidence": 0.3}
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    return {"primary": ranked[0][0], "categories": [d for d, _ in ranked[:3]], "confidence": round(min(0.99, ranked[0][1] / max(1, sum(scores.values()))), 3)}
async def backfill_domain(limit=5000):
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        atoms = conn.execute("SELECT a.id, a.name, a.code FROM atoms a WHERE NOT EXISTS (SELECT 1 FROM meta_events m WHERE m.target_id = a.id AND m.namespace = 'domain') LIMIT %s", (limit,)).fetchall()
    written = 0
    for atom_id, name, code in atoms:
        meta.write_meta("atoms", atom_id, "domain", classify_domain(name, [], code or ""), written_by="classifier_v1"); written += 1
    return written
