"""
bulk_publish.py — Publicación masiva a redes sociales vía Postiz Public API.

Uso:
    export POSTIZ_URL="http://localhost:4007"          # o https://api.postiz.com
    export POSTIZ_API_KEY="pk_xxx"                     # Settings → API en la UI
    python bulk_publish.py posts.example.csv

Formato del CSV (una fila = un post):
    integration_ids   |  content                 |  media_path        |  scheduled_at
    id1;id2;id3       |  "Copy del post 🚀"      |  ./portada.jpg     |  2026-08-25T10:00:00Z

- integration_ids  = IDs de las cuentas conectadas (separadas por ';')
                     Obtenlos con:  GET /public/v1/integrations
- media_path       = ruta local a la portada (imagen/vídeo); vacío = sin media
- scheduled_at     = ISO-8601 UTC; vacío = publicar ya

Los IDs de integración salen de la UI de Postiz o de:
    curl -H "Authorization: $POSTIZ_API_KEY" $POSTIZ_URL/public/v1/integrations
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
API_KEY = os.getenv("POSTIZ_API_KEY", "")
BASE = f"{POSTIZ_URL}/public/v1"

if not API_KEY:
    sys.exit("Falta POSTIZ_API_KEY en el entorno.")

HEADERS = {"Authorization": API_KEY}


def upload_media(path: str) -> dict:
    """Sube un fichero al bucket de Postiz y devuelve {id, path}."""
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        raise FileNotFoundError(p)
    with p.open("rb") as fh:
        r = requests.post(
            f"{BASE}/upload",
            headers=HEADERS,
            files={"file": (p.name, fh)},
            timeout=120,
        )
    r.raise_for_status()
    return r.json()


def list_integrations() -> list[dict]:
    r = requests.get(f"{BASE}/integrations", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def create_post(
    integration_ids: list[str],
    content: str,
    media: list[dict] | None = None,
    scheduled_at: str | None = None,
    kind: str = "draft",
    ig_reel_mode: str | None = None,   # 'REELS' | 'TRIAL_REEL' | None
) -> dict:
    """
    kind:
      - 'now'      → publica de inmediato
      - 'schedule' → usa scheduled_at
      - 'draft'    → guarda en borrador
    """
    when = scheduled_at or datetime.now(timezone.utc).isoformat()
    body = {
        "type": kind,
        "date": when,
        "shortLink": False,
        "posts": [
            {
                "integration": {"id": iid},
                "value": [
                    {
                        "content": content,
                        "image": media or [],
                        **({"settings": {"post_type": ig_reel_mode}} if ig_reel_mode else {}),
                    }
                ],
            }
            for iid in integration_ids
        ],
    }
    r = requests.post(
        f"{BASE}/posts",
        headers={**HEADERS, "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=60,
    )
    if r.status_code >= 400:
        print(" respuesta:", r.text[:400])
    r.raise_for_status()
    return r.json()


def run(csv_path: str) -> None:
    integrations = {i["id"]: i.get("name", i["id"]) for i in list_integrations()}
    print(f"[i] {len(integrations)} cuentas conectadas en Postiz")

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    for idx, row in enumerate(rows, 1):
        ids = [i.strip() for i in (row.get("integration_ids") or "").split(";") if i.strip()]
        if not ids:
            print(f"[{idx}] sin integration_ids — saltando")
            continue

        unknown = [i for i in ids if i not in integrations]
        if unknown:
            print(f"[{idx}] IDs desconocidos: {unknown}")

        media = []
        if row.get("media_path"):
            try:
                up = upload_media(row["media_path"])
                media = [{"path": up.get("path") or up.get("url"), "id": up.get("id")}]
                print(f"[{idx}] media subido: {up.get('id')}")
            except Exception as e:
                print(f"[{idx}] error subiendo media: {e}")
                continue

        scheduled = row.get("scheduled_at") or None
        kind = "schedule" if scheduled else "now"

        # Detecta Reel: si media es .mp4 y toca IG/TikTok/YT/FB, marca modo Reel/Trial
        reel_mode = None
        if row.get("media_path", "").lower().endswith(".mp4"):
            reel_mode = "TRIAL_REEL" if "trial" in row.get("content", "").lower() else "REELS"

        try:
            resp = create_post(ids, row.get("content", ""), media, scheduled,
                               kind=kind, ig_reel_mode=reel_mode)
            print(f"[{idx}] OK ({kind}) → {len(ids)} redes  ids={ids}")
        except requests.HTTPError as e:
            print(f"[{idx}] HTTP {e.response.status_code}: {e.response.text[:200]}")
        except Exception as e:
            print(f"[{idx}] error: {e}")

        time.sleep(1)  # respetar rate-limit (90/h por defecto)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Uso: python bulk_publish.py <posts.csv>")
    run(sys.argv[1])
