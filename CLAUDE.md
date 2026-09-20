# Contexto del proyecto

## Dos dashboards distintos — no confundirlos

| | Dónde vive | Qué es |
|---|---|---|
| **Este repo** (`metricshub-dashboard`) | Render → `metricshub-dashboard.onrender.com` | Métricas de negocio: Meta Ads, Stripe, TrainingPeaks, Inversiones |
| **Dashboard de inversiones personal** | Servidor **hitcloser** → `ximescanellas.com/inversiones/` | Cartera personal. **No es WordPress ni está en este repo.** |

## Archivo que se sirve en producción

`api_proxy.py` sirve **`dashboard.html`** en `/`.

`index.html` quedó **huérfano** tras el rename del commit `edbee6c`: es más grande y
tiene más secciones, pero **nadie lo sirve**. Cualquier cambio de UI va a
`dashboard.html`, no a `index.html`.

## Acceso a hitcloser

- Panel Plesk: `https://srv-comp-001.hitcloser.net:8443/smb/web/view` (IP `94.23.81.13`)
- Usuario: `ximescanellas@hotmail.com`
- Contraseña: **no se guarda aquí** — pedirla al usuario o usar un gestor de contraseñas.
  Este repo tiene escaneo de secretos (GitGuardian) y es público al proxy de git.

**El entorno remoto de Claude NO alcanza hitcloser.** El proxy autoriza el CONNECT,
pero el servidor resetea la conexión tras el Client Hello — falla igual en 8443 (TLS),
8880 (HTTP plano), 443 y por IP directa, así que es un bloqueo por IP de origen del
propio hosting, no un problema de certificados ni de política del proxy.

Para tocar el dashboard de inversiones: pedir el HTML al usuario, editarlo y
devolvérselo para que lo suba él desde el panel.

## Datos de inversiones

La sección de Inversiones de `dashboard.html` lleva los datos incrustados en
`INV_TX` / `INV_FUNDS` / `INV_PP_TX`. Las filas con `est:true` tienen las
participaciones estimadas por interpolación de VL (se marcan con ≈ en la tabla),
porque el extracto de movimientos de MyInvestor no las muestra.
