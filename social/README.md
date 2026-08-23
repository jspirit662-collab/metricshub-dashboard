# Social — publicación masiva multi-red

Stack basado en **Postiz** (OSS, 35k ⭐) para publicar copies + portadas a
las 14 redes que soporta: **Instagram, TikTok, YouTube, LinkedIn, X,
Facebook, Threads, Pinterest, Bluesky, Reddit, Mastodon, Discord, Slack,
Dribbble**.

> GHL no forma parte de este flujo: GHL sirve para CRM/leads/pipeline
> (ya integrado en `api_proxy.py`) y no publica en redes de terceros por
> sí mismo. Para eso está Postiz.

---

## 1. Levantar el stack

```bash
cd social
cp postiz.env.example postiz.env
# editar postiz.env y rellenar JWT_SECRET + las OAuth de las redes que uses
docker compose up -d
open http://localhost:4007
```

Primer arranque: te pide crear el usuario admin.

## 2. Conectar cuentas

`Launches → Add Channel` en la UI. Cada red pide OAuth de la app que
hayas dado de alta en su portal de developers (las claves van en
`postiz.env`).

## 3. Generar API key

`Settings → Public API → Generate` → guarda la clave.

## 4. Publicación masiva

```bash
export POSTIZ_URL="http://localhost:4007"
export POSTIZ_API_KEY="pk_xxx"

# obtener los IDs de las cuentas conectadas
curl -H "Authorization: $POSTIZ_API_KEY" $POSTIZ_URL/public/v1/integrations

# editar posts.example.csv con esos IDs, copies y rutas de portadas
python bulk_publish.py posts.example.csv
```

Formato del CSV:

| columna           | qué es                                                     |
|-------------------|------------------------------------------------------------|
| `integration_ids` | IDs de cuentas separados por `;` (una fila = una tanda)    |
| `content`         | Copy del post (mismo para todas las redes de esa fila)     |
| `media_path`      | Portada local — imagen o vídeo. Vacío = sin media          |
| `scheduled_at`    | ISO-8601 UTC. Vacío = publicar ya                          |

Rate limit por defecto: **90 req/h** en self-host (100 en el cloud). El
script deja 1 s entre posts para respetarlo.

## 5. Wizard AI (copies + portadas automáticos)

`ai_publish.py` toma un CSV de **temas** y para cada uno:

1. Genera la **portada** con `gpt-image-1` (1024×1024) adaptada al
   estilo que indiques.
2. Detecta la red de cada `integration_id` y genera un **copy
   específico**: LinkedIn largo con CTA, X con gancho y ≤260 ch, IG con
   emojis y 8-12 hashtags, Threads hilo-friendly, TikTok energético,
   Reddit sin hashtags, etc. — ver `NETWORK_SPECS` en el script.
3. Sube la portada al bucket de Postiz y crea el post con contenido
   distinto por red en una sola llamada a `/public/v1/posts`.

```bash
export POSTIZ_URL="http://localhost:4007"
export POSTIZ_API_KEY="pk_xxx"
export OPENAI_API_KEY="sk_xxx"
python ai_publish.py topics.example.csv
```

Columnas del CSV:

| columna           | qué es                                                          |
|-------------------|-----------------------------------------------------------------|
| `theme`           | Idea o tema central del post                                    |
| `tone`            | Tono deseado (cercano, técnico, humor, autoridad…)              |
| `integration_ids` | IDs Postiz de las cuentas destino (`;` separator)               |
| `scheduled_at`    | ISO-8601 UTC; vacío = publicar ya                               |
| `cover_style`     | Hint de estilo visual para la portada AI                        |

Las portadas generadas se guardan también en `./generated-covers/` por
si querés reutilizarlas o editarlas antes de publicar.

Alternativa dentro de la UI de Postiz: `AI Assistant` en el editor
(copies) y `Design with AI` con Polotno (portadas) — ambos usan tu
`OPENAI_API_KEY` de `postiz.env`.

## 6. Instagram Reels (incluidos Reels de prueba)

Postiz publica Reels **nativos** subiendo un `.mp4` al editor — usa la
Instagram Graph API, así que:

- La cuenta IG tiene que ser **Business o Creator** y estar vinculada a
  una **Página de Facebook** (personal no vale, es limitación de Meta).
- Sube el vídeo una vez y márcalo para IG Reels + FB Reels + TikTok +
  YT Shorts en el mismo post: Postiz publica versión nativa en cada red.
- **Trial Reels**: en las opciones del post activa "Trial Reel" y el
  Reel sale con alcance limitado a no-seguidores; puedes promocionarlo
  a alcance completo manual o automáticamente según rendimiento. Ideal
  para A/B testing de portadas y copies antes de sacarlo en frío.
- Carrusel: varias imágenes/vídeos en un mismo post → carrusel nativo
  (caption hasta 2.200 caracteres).
- Stories: imágenes o vídeos como Stories nativas (multi-frame se
  publican como Stories separadas).

Para lanzar Reels desde `bulk_publish.py`, apunta `media_path` a un
`.mp4` (H.264, ≤ 90 s, ≤ 100 MB, aspect 9:16). Ejemplo en el CSV.

## 7. Integraciones adicionales

- **n8n / Make / Zapier**: nodo oficial de Postiz — ideal si el disparo
  viene de otro sistema (Airtable, GHL, formulario…).
- **MCP**: existe `mcp-postiz` (comunidad) para que este mismo Claude
  publique directamente sin el script.

---

### Diagrama de flujo

```
 Airtable/GHL/CSV  ──▶  bulk_publish.py  ──▶  Postiz Public API
                                                    │
                                       ┌────────────┼────────────┐
                                       ▼            ▼            ▼
                                    IG/TikTok    LinkedIn/X    YouTube/…
```
