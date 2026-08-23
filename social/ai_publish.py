"""
ai_publish.py — Wizard AI que genera copies por red + portadas y las
publica en Postiz.

Uso:
    export POSTIZ_URL="http://localhost:4007"
    export POSTIZ_API_KEY="pk_xxx"
    export OPENAI_API_KEY="sk_xxx"
    python ai_publish.py topics.example.csv

Formato CSV (un tema = una tanda multi-red):
    theme                | tone      | integration_ids   | scheduled_at         | cover_style
    "Lanzamiento X"      | "cercano" | id1;id2;id3       | 2026-08-25T10:00:00Z | "minimal, azul"

- theme         : idea/tema central del post
- tone          : tono (cercano, técnico, humor, autoridad…)
- integration_ids: IDs Postiz de las cuentas destino (';' separator)
- scheduled_at  : ISO-8601 UTC, vacío = publicar ya
- cover_style   : hint de estilo para la portada (opcional)

Para cada red, adapta el copy al formato:
  - x/twitter  → 260 chars máx, 1-2 hashtags, gancho fuerte
  - linkedin   → 1200 chars, estructura de párrafos, CTA final
  - instagram  → 400 chars, 8-12 hashtags, emojis
  - facebook   → 500 chars, tono conversacional
  - threads    → 300 chars, hilo-friendly
  - tiktok     → 150 chars + trending sound hint
  - youtube    → título 70 chars + descripción 500 chars
  - pinterest  → título 100 chars + descripción con keywords
  - reddit     → título específico de subreddit, sin hashtags
  - bluesky    → 300 chars, sin corporate speak
  - mastodon   → 500 chars, con CW si aplica

Portada: 1024x1024 (o 1024x1792 para Stories/Reels si el tema pide vídeo).
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

POSTIZ_URL = os.getenv("POSTIZ_URL", "http://localhost:4007").rstrip("/")
POSTIZ_KEY = os.getenv("POSTIZ_API_KEY", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

for name, val in [("POSTIZ_API_KEY", POSTIZ_KEY), ("OPENAI_API_KEY", OPENAI_KEY)]:
    if not val:
        sys.exit(f"Falta {name} en el entorno.")

POSTIZ_BASE = f"{POSTIZ_URL}/public/v1"
POSTIZ_HEADERS = {"Authorization": POSTIZ_KEY}
OAI_BASE = "https://api.openai.com/v1"
OAI_HEADERS = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}

# ── Perfiles de copy por red ─────────────────────────────────────────────
NETWORK_SPECS: dict[str, dict] = {
    "x":         {"max": 260,  "hashtags": 2,  "hint": "gancho fuerte primera línea"},
    "twitter":   {"max": 260,  "hashtags": 2,  "hint": "gancho fuerte primera línea"},
    "linkedin":  {"max": 1200, "hashtags": 3,  "hint": "estructura P1-P2-P3 + CTA"},
    "instagram": {"max": 400,  "hashtags": 10, "hint": "emojis, hashtags al final"},
    "facebook":  {"max": 500,  "hashtags": 2,  "hint": "tono conversacional"},
    "threads":   {"max": 300,  "hashtags": 1,  "hint": "hilo-friendly, primera parte"},
    "tiktok":    {"max": 150,  "hashtags": 5,  "hint": "trending, directo, energético"},
    "youtube":   {"max": 500,  "hashtags": 3,  "hint": "título 70ch + descripción SEO"},
    "pinterest": {"max": 200,  "hashtags": 5,  "hint": "keywords + descripción visual"},
    "reddit":    {"max": 300,  "hashtags": 0,  "hint": "sin hashtags, tono foro"},
    "bluesky":   {"max": 300,  "hashtags": 1,  "hint": "sin corporate speak"},
    "mastodon":  {"max": 500,  "hashtags": 3,  "hint": "puede usar CW si aplica"},
    "discord":   {"max": 2000, "hashtags": 0,  "hint": "markdown permitido"},
    "slack":     {"max": 2000, "hashtags": 0,  "hint": "markdown permitido"},
    "dribbble":  {"max": 300,  "hashtags": 3,  "hint": "descripción de diseño"},
}


def list_integrations() -> list[dict]:
    r = requests.get(f"{POSTIZ_BASE}/integrations", headers=POSTIZ_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def detect_network(integration: dict) -> str:
    """Extrae la red desde el objeto integration de Postiz."""
    for key in ("identifier", "providerIdentifier", "provider", "type", "name"):
        v = (integration.get(key) or "").lower()
        for net in NETWORK_SPECS:
            if net in v:
                return net
    return "instagram"  # fallback razonable


def generate_copy(theme: str, tone: str, network: str) -> str:
    spec = NETWORK_SPECS.get(network, NETWORK_SPECS["instagram"])
    system = (
        "Eres un copywriter senior de social media. Adaptas mensajes al "
        "formato y cultura de cada red. Escribes en español natural, sin "
        "clichés ni jerga de marketing. NO uses comillas alrededor del "
        "output. Devuelve SOLO el copy final, listo para publicar."
    )
    user = (
        f"Red: {network}\n"
        f"Tema: {theme}\n"
        f"Tono: {tone}\n"
        f"Máx caracteres: {spec['max']}\n"
        f"Hashtags: {spec['hashtags']} (máx)\n"
        f"Notas: {spec['hint']}"
    )
    body = {
        "model": "gpt-4o-mini",
        "temperature": 0.8,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    r = requests.post(f"{OAI_BASE}/chat/completions", headers=OAI_HEADERS,
                      data=json.dumps(body), timeout=60)
    r.raise_for_status()
    text = r.json()["choices"][0]["message"]["content"].strip()
    return text[:spec["max"]]


def generate_cover(theme: str, style: str, out_dir: Path) -> Path:
    prompt = (
        f"Portada para post de redes sociales sobre: {theme}. "
        f"Estilo: {style or 'moderno, colores vibrantes, alto contraste'}. "
        "Composición limpia, texto mínimo, foco central. "
        "Sin logos ni marcas registradas."
    )
    body = {
        "model": "gpt-image-1",
        "prompt": prompt,
        "size": "1024x1024",
        "n": 1,
    }
    r = requests.post(f"{OAI_BASE}/images/generations", headers=OAI_HEADERS,
                      data=json.dumps(body), timeout=180)
    r.raise_for_status()
    import base64
    b64 = r.json()["data"][0]["b64_json"]
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = "".join(c if c.isalnum() else "-" for c in theme.lower())[:40]
    path = out_dir / f"{slug}-{int(time.time())}.png"
    path.write_bytes(base64.b64decode(b64))
    return path


def upload_media(path: Path) -> dict:
    with path.open("rb") as fh:
        r = requests.post(f"{POSTIZ_BASE}/upload", headers=POSTIZ_HEADERS,
                          files={"file": (path.name, fh)}, timeout=120)
    r.raise_for_status()
    return r.json()


def create_multi_post(
    per_network_content: dict[str, tuple[str, str]],  # {int_id: (network, copy)}
    media_ref: dict,
    scheduled_at: str | None,
) -> dict:
    when = scheduled_at or datetime.now(timezone.utc).isoformat()
    body = {
        "type": "schedule" if scheduled_at else "now",
        "date": when,
        "shortLink": False,
        "posts": [
            {
                "integration": {"id": iid},
                "value": [
                    {
                        "content": copy_text,
                        "image": [media_ref] if media_ref else [],
                    }
                ],
            }
            for iid, (net, copy_text) in per_network_content.items()
        ],
    }
    r = requests.post(f"{POSTIZ_BASE}/posts",
                      headers={**POSTIZ_HEADERS, "Content-Type": "application/json"},
                      data=json.dumps(body), timeout=60)
    if r.status_code >= 400:
        print(" respuesta:", r.text[:400])
    r.raise_for_status()
    return r.json()


def run(csv_path: str) -> None:
    integrations = {i["id"]: i for i in list_integrations()}
    print(f"[i] {len(integrations)} cuentas conectadas")

    out_dir = Path("./generated-covers")

    with open(csv_path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    for idx, row in enumerate(rows, 1):
        theme = row.get("theme", "").strip()
        tone = row.get("tone", "cercano").strip()
        style = row.get("cover_style", "").strip()
        ids = [i.strip() for i in (row.get("integration_ids") or "").split(";") if i.strip()]
        scheduled = row.get("scheduled_at") or None

        if not theme or not ids:
            print(f"[{idx}] sin tema o sin ids — saltando")
            continue

        # 1) Portada AI
        try:
            cover = generate_cover(theme, style, out_dir)
            print(f"[{idx}] portada generada: {cover}")
        except Exception as e:
            print(f"[{idx}] fallo generando portada: {e}")
            continue

        # 2) Subir portada a Postiz
        try:
            up = upload_media(cover)
            media_ref = {"path": up.get("path") or up.get("url"), "id": up.get("id")}
        except Exception as e:
            print(f"[{idx}] fallo subiendo portada: {e}")
            continue

        # 3) Copy por red
        per_net: dict[str, tuple[str, str]] = {}
        for iid in ids:
            integ = integrations.get(iid)
            if not integ:
                print(f"[{idx}]  ID desconocido {iid} — saltando")
                continue
            net = detect_network(integ)
            try:
                copy_text = generate_copy(theme, tone, net)
                per_net[iid] = (net, copy_text)
                print(f"[{idx}]  {net:10s} → {copy_text[:80]}…")
            except Exception as e:
                print(f"[{idx}]  fallo copy {net}: {e}")

        if not per_net:
            continue

        # 4) Publicar
        try:
            create_multi_post(per_net, media_ref, scheduled)
            print(f"[{idx}] publicado en {len(per_net)} redes")
        except requests.HTTPError as e:
            print(f"[{idx}] HTTP {e.response.status_code}: {e.response.text[:200]}")
        except Exception as e:
            print(f"[{idx}] error: {e}")

        time.sleep(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Uso: python ai_publish.py <topics.csv>")
    run(sys.argv[1])
