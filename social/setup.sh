#!/usr/bin/env bash
# setup.sh — arranque de Postiz en tu ordenador
#
# Uso:
#   cd social
#   ./setup.sh
#
# Hace:
#   1. Comprueba que Docker está instalado y corriendo
#   2. Crea postiz.env con un JWT_SECRET aleatorio si no existe
#   3. Levanta el stack (Postiz + Postgres + Redis)
#   4. Espera a que la UI responda y la abre en el navegador
#
# Si ya lo tenías corriendo, simplemente aplica cambios y sale.

set -euo pipefail

cd "$(dirname "$0")"

# ── 1. Docker ──────────────────────────────────────────────
if ! command -v docker >/dev/null 2>&1; then
  cat <<'EOF' >&2
✖  Docker no está instalado.

   macOS:   descarga Docker Desktop → https://docs.docker.com/desktop/install/mac-install/
   Windows: descarga Docker Desktop → https://docs.docker.com/desktop/install/windows-install/
   Linux:   curl -fsSL https://get.docker.com | sh

Después re-ejecuta ./setup.sh
EOF
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "✖  Docker está instalado pero no corriendo. Abre Docker Desktop y vuelve a intentarlo." >&2
  exit 1
fi

# compose v2 (docker compose) o v1 (docker-compose)
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "✖  Falta el plugin 'compose'. Actualiza Docker Desktop." >&2
  exit 1
fi

# ── 2. postiz.env ──────────────────────────────────────────
if [[ ! -f postiz.env ]]; then
  echo "→ Creando postiz.env desde la plantilla…"
  cp postiz.env.example postiz.env
  JWT=$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))' 2>/dev/null \
        || openssl rand -base64 48 | tr -d '\n')
  # sed portable Mac/Linux
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s|JWT_SECRET=.*|JWT_SECRET=\"$JWT\"|" postiz.env
  else
    sed -i "s|JWT_SECRET=.*|JWT_SECRET=\"$JWT\"|" postiz.env
  fi
  echo "✔ postiz.env creado con JWT_SECRET aleatorio."
  echo "  Rellena las OAuth de las redes cuando las vayas a conectar."
else
  echo "→ postiz.env ya existe — respetando tu configuración."
fi

# ── 3. Levantar stack ─────────────────────────────────────
echo "→ Descargando imágenes y arrancando (puede tardar 1-2 min la primera vez)…"
$DC pull
$DC up -d

# ── 4. Esperar a que la UI responda ────────────────────────
URL="http://localhost:4007"
echo -n "→ Esperando a que Postiz esté listo en $URL "
for i in {1..90}; do
  if curl -fsS --max-time 2 "$URL" >/dev/null 2>&1; then
    echo " ✔"
    break
  fi
  echo -n "."
  sleep 2
done
echo

# ── 5. Abrir navegador ────────────────────────────────────
if command -v open >/dev/null 2>&1; then       # macOS
  open "$URL"
elif command -v xdg-open >/dev/null 2>&1; then  # Linux
  xdg-open "$URL"
elif command -v start >/dev/null 2>&1; then     # Git Bash / WSL
  start "$URL"
fi

cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✔  Postiz corriendo en $URL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Siguientes pasos:

  1. Abre $URL y crea el usuario admin.
  2. Settings → Add Channel → conecta las redes con OAuth.
     (necesitas rellenar sus claves en postiz.env — mira la plantilla)
  3. Settings → Public API → Generate → guarda la API key.
  4. Para publicar en masa desde el CSV:

       export POSTIZ_URL=$URL
       export POSTIZ_API_KEY=pk_xxx
       python bulk_publish.py posts.example.csv

Comandos útiles:

  ./setup.sh           → re-lanzar (idempotente)
  $DC logs -f postiz    → ver logs en vivo
  $DC restart postiz    → reiniciar solo Postiz (tras editar env)
  $DC down              → parar todo (los datos se guardan)
  $DC down -v           → parar y borrar TODO (⚠ pierdes cuentas y posts)
EOF
