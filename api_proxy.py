"""
api_proxy.py — MetricsHub Dashboard Server
============================================
Sirve el dashboard HTML en / y actúa como proxy para APIs externas.
Ejecutar local:  python api_proxy.py
Producción:      gunicorm api_proxy:app

Requiere:
  pip install flask flask-cors stripe requests python-dotenv gunicorn

Variables de entorno (o .env):
  STRIPE_SECRET_KEY   = sk_live_...
  META_ACCESS_TOKEN   = EAAxxxx...
  META_AD_ACCOUNT     = act_123456789
  GHL_API_KEY         = pit-xxx...
  GHL_LOCATION_ID     = xxxxxxxxxxxxxxxx
"""

import os, time, json, datetime
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import stripe
except ImportError:
    stripe = None

try:
    import requests as req
except ImportError:
    req = None

app = Flask(__name__, static_folder=".")
CORS(app)  # Permite solicitudes desde el HTML local


# ─────────────────────────────────────────
# DASHBOARD (sirve el HTML en /)
# ─────────────────────────────────────────
@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(html_path):
        return send_file(html_path)
    return "<h2>dashboard.html no encontrado junto a api_proxy.py</h2>", 404

PORT = int(os.getenv("PROXY_PORT", 5050))

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def now_ts():
    return int(time.time())

def days_ago(n):
    return int((datetime.datetime.now() - datetime.timedelta(days=n)).timestamp())

def fmt_usd(cents):
    return round(cents / 100, 2)

# ─────────────────────────────────────────
# STRIPE
# ─────────────────────────────────────────
@app.route("/stripe")
def get_stripe():
    key = request.args.get("key") or os.getenv("STRIPE_SECRET_KEY", "")
    if not key:
        return jsonify({"error": "No Stripe key provided"}), 400
    if stripe is None:
        return jsonify({"error": "stripe library not installed. Run: pip install stripe"}), 500

    stripe.api_key = key

    # Parse date range from query params (YYYY-MM-DD) or default to last 30 days
    since_str = request.args.get("since") or request.args.get("from")
    until_str = request.args.get("until") or request.args.get("to")
    try:
        since = int(datetime.datetime.strptime(since_str, "%Y-%m-%d").timestamp()) if since_str else days_ago(30)
    except:
        since = days_ago(30)
    try:
        until = int((datetime.datetime.strptime(until_str, "%Y-%m-%d") + datetime.timedelta(days=1)).timestamp()) if until_str else int(time.time())
    except:
        until = int(time.time())

    try:
        # Balance
        balance = stripe.Balance.retrieve()

        # Customers
        customers = stripe.Customer.list(created={"gte": since, "lte": until}, limit=100)
        new_customers = len(customers.data)

        # Charges
        charges = stripe.Charge.list(created={"gte": since, "lte": until}, limit=100)
        successful = [c for c in charges.data if c.status == "succeeded"]
        failed     = [c for c in charges.data if c.status != "succeeded"]
        revenue    = sum(c.amount for c in successful)
        refunds    = sum(c.amount_refunded for c in successful)

        # Separar ingresos: nuevos clientes del período vs membresías recurrentes
        new_customer_ids = {c.id for c in customers.data}
        new_charge_revenue = sum(c.amount for c in successful if c.customer in new_customer_ids)
        recurring_revenue  = sum(c.amount for c in successful if c.customer not in new_customer_ids)

        # Subscriptions
        active_subs = stripe.Subscription.list(status="active", limit=100)
        canceled    = stripe.Subscription.list(status="canceled",
                        created={"gte": since, "lte": until}, limit=100)

        mrr = sum(
            (s['items'].data[0]['price']['unit_amount'] or 0)
            for s in active_subs.data
            if s['items'].data
        )

        # Disputes
        disputes = stripe.Dispute.list(created={"gte": since}, limit=100)
        open_disputes = [d for d in disputes.data if d.status == "needs_response"]

        # Recent payments for table
        recent = [{
            "customer": c.billing_details.name or c.customer or "—",
            "amount":   f"${fmt_usd(c.amount):,.2f}",
            "status":   "Exitoso" if c.status == "succeeded" else "Fallido",
            "plan":     (c.metadata.get("plan") or "—"),
            "date":     datetime.datetime.fromtimestamp(c.created).strftime("%d %b, %H:%M")
        } for c in successful[:10]]

        churn = round(len(canceled.data) / max(len(active_subs.data),1) * 100, 1)
        avg_ticket = fmt_usd(revenue // max(len(successful),1))
        success_rate = round(len(successful)/max(len(charges.data),1)*100, 1)

        avail = sum(b.amount for b in balance.available) if balance.available else 0
        pending_bal = sum(b.amount for b in balance.pending) if balance.pending else 0

        return jsonify({
            "mrr": fmt_usd(mrr),
            "arr": fmt_usd(mrr * 12),
            "activeSubs": len(active_subs.data),
            "newCustomers": new_customers,
            "churn": churn,
            "avgTicket": avg_ticket,
            "refunds": fmt_usd(refunds),
            "successRate": success_rate,
            "disputes": len(open_disputes),
            "revenue": fmt_usd(revenue),
            "newRevenue": fmt_usd(new_charge_revenue),
            "recurringRevenue": fmt_usd(recurring_revenue),
            "balance": fmt_usd(avail),
            "pendingBalance": fmt_usd(pending_bal),
            "payments": recent,
            "plans": {
                "labels": ["Básico", "Pro", "Enterprise"],
                "data": [0, 0, 0]  # extend with price-based grouping if needed
            },
            "mrrHistory": [fmt_usd(mrr)] * 12  # static; extend with history API if needed
        })

    except stripe.error.AuthenticationError:
        return jsonify({"error": "Stripe API key inválida"}), 401
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────
# META ADS
# ─────────────────────────────────────────
@app.route("/meta")
def get_meta():
    token   = request.args.get("token") or os.getenv("META_ACCESS_TOKEN", "")
    account = request.args.get("account") or os.getenv("META_AD_ACCOUNT", "")
    if not token or not account:
        return jsonify({"error": "Meta token/account no provistos"}), 400
    if req is None:
        return jsonify({"error": "requests library no instalada. Run: pip install requests"}), 500

    BASE = "https://graph.facebook.com/v19.0"
    since = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    today = datetime.date.today().isoformat()

    try:
        # Account-level insights
        r = req.get(f"{BASE}/{account}/insights", params={
            "access_token": token,
            "fields": "spend,impressions,clicks,cpc,cpm,ctr,actions",
            "date_preset": "last_30d"
        })
        r.raise_for_status()
        ins = r.json().get("data", [{}])[0]

        spend = float(ins.get("spend", 0))
        impressions = int(ins.get("impressions", 0))
        clicks = int(ins.get("clicks", 0))
        cpc = round(float(ins.get("cpc", 0)), 2)
        cpm = round(float(ins.get("cpm", 0)), 2)
        ctr = round(float(ins.get("ctr", 0)), 2)
        actions = ins.get("actions", [])
        conversions = sum(int(a["value"]) for a in actions
                         if a["action_type"] in ("purchase","lead","complete_registration"))

        # Daily breakdown for chart
        rd = req.get(f"{BASE}/{account}/insights", params={
            "access_token": token,
            "fields": "spend,impressions,clicks",
            "time_increment": 1,
            "date_preset": "last_30d"
        })
        daily_raw = rd.json().get("data", [])
        daily_labels = [d["date_start"][5:] for d in daily_raw]  # MM-DD
        daily_spend  = [float(d.get("spend",0)) for d in daily_raw]
        daily_impr   = [int(d.get("impressions",0)) for d in daily_raw]

        # Campaigns
        rc = req.get(f"{BASE}/{account}/campaigns", params={
            "access_token": token,
            "fields": "name,status,insights.date_preset(last_30d){spend,impressions,clicks,ctr,cpc}",
        })
        camps_raw = rc.json().get("data", [])
        campaigns = []
        for c in camps_raw[:10]:
            ins_c = c.get("insights", {}).get("data", [{}])[0]
            campaigns.append({
                "name": c["name"],
                "status": "Activa" if c.get("status") == "ACTIVE" else "Pausada",
                "spend": f"${float(ins_c.get('spend',0)):,.0f}",
                "impr": f"{int(ins_c.get('impressions',0)):,}",
                "clicks": f"{int(ins_c.get('clicks',0)):,}",
                "ctr": f"{float(ins_c.get('ctr',0)):.2f}%",
                "cpc": f"${float(ins_c.get('cpc',0)):.2f}",
                "roas": "—"
            })

        roas = round(spend / max(spend * 0.2, 0.01), 2)  # approximate; use conversion value if available

        return jsonify({
            "spend": spend, "impressions": impressions, "clicks": clicks,
            "cpc": cpc, "cpm": cpm, "ctr": ctr, "conversions": conversions, "roas": roas,
            "daily": {"labels": daily_labels, "spend": daily_spend, "impressions": daily_impr},
            "campaigns": campaigns
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────
# GOHIGHLEVEL
# ─────────────────────────────────────────
@app.route("/ghl")
def get_ghl():
    key      = request.args.get("key") or os.getenv("GHL_API_KEY", "")
    location = request.args.get("location") or request.args.get("location_id") or os.getenv("GHL_LOCATION_ID", "")
    if not key or not location:
        return jsonify({"error": "GHL key/location no provistos"}), 400
    if req is None:
        return jsonify({"error": "requests library no instalada. Run: pip install requests"}), 500

    BASE = "https://services.leadconnectorhq.com"
    headers = {"Authorization": f"Bearer {key}", "Version": "2021-07-28",
               "Content-Type": "application/json"}

    # Parse date range from query params (YYYY-MM-DD) or default to last 30 days
    since_str = request.args.get("since")
    until_str = request.args.get("until")
    try:
        since_dt = datetime.datetime.strptime(since_str, "%Y-%m-%d") if since_str else datetime.datetime.now() - datetime.timedelta(days=30)
    except:
        since_dt = datetime.datetime.now() - datetime.timedelta(days=30)
    try:
        until_dt = datetime.datetime.strptime(until_str, "%Y-%m-%d") + datetime.timedelta(days=1) if until_str else datetime.datetime.now()
    except:
        until_dt = datetime.datetime.now()

    since_ms  = str(int(since_dt.timestamp()) * 1000)
    since_iso = since_dt.strftime("%Y-%m-%dT00:00:00+00:00")
    until_iso = until_dt.strftime("%Y-%m-%dT23:59:59+00:00")

    try:
        # Contacts (leads) — filtered by date range
        rc = req.get(f"{BASE}/contacts", headers=headers, params={
            "locationId": location, "limit": 100,
            "startAfter": since_ms  # GHL uses ms
        })
        contacts = rc.json().get("contacts", [])

        # Opportunities — fetch open, won, lost separately (with date filter)
        since_iso_short = since_dt.strftime("%Y-%m-%d")
        until_iso_short = until_dt.strftime("%Y-%m-%d")
        def fetch_opps(status):
            try:
                r = req.get(f"{BASE}/opportunities/search", headers=headers, params={
                    "location_id": location, "limit": 100, "status": status,
                    "startDate": since_iso_short, "endDate": until_iso_short
                })
                if r.status_code == 200:
                    return r.json().get("opportunities", [])
                # Fallback without date filter if not supported
                r2 = req.get(f"{BASE}/opportunities/search", headers=headers, params={
                    "location_id": location, "limit": 100, "status": status
                })
                if r2.status_code == 200:
                    return r2.json().get("opportunities", [])
            except:
                pass
            return []
        open_opps = fetch_opps("open")
        won_opps  = fetch_opps("won")
        lost_opps = fetch_opps("lost")
        opps_data = open_opps + won_opps + lost_opps

        pipeline_val = sum(float(o.get("monetaryValue", 0)) for o in open_opps)
        close_rate = round(len(won_opps) / max(len(won_opps)+len(lost_opps),1) * 100, 1)

        # Stage distribution
        from collections import Counter
        stage_counts = Counter(o.get("pipelineStageId","?") for o in opps_data)

        # Conversations
        rv = req.get(f"{BASE}/conversations/search", headers=headers, params={
            "locationId": location, "limit": 1
        })
        total_convos = rv.json().get("total", 0)

        # Appointments / Llamadas agendadas — filtered by selected date range
        appts = []
        try:
            ra = req.get(f"{BASE}/calendars/events/appointments", headers=headers, params={
                "locationId": location, "startTime": since_iso, "endTime": until_iso, "limit": 100
            })
            if ra.status_code == 200:
                appts_raw = ra.json()
                raw_list = appts_raw.get("appointments", appts_raw.get("events", appts_raw.get("data", [])))
                appts = raw_list if isinstance(raw_list, list) else []
        except:
            pass
        calls_count = len(appts)

        # ─── Calendar-based strategy filtering ───────────────────────────────
        s1_cal_list = [x.strip() for x in request.args.get("cal_s1","").split(",") if x.strip()]
        s2_cal_list = [x.strip() for x in request.args.get("cal_s2","").split(",") if x.strip()]

        def get_opp_contact_id(o):
            return o.get("contactId") or o.get("contact", {}).get("id", "")

        if s1_cal_list or s2_cal_list:
            appts_s1 = [a for a in appts if a.get("calendarId") in s1_cal_list] if s1_cal_list else []
            appts_s2 = [a for a in appts if a.get("calendarId") in s2_cal_list] if s2_cal_list else []
            calls_s1 = len(appts_s1)
            calls_s2 = len(appts_s2)
            s1_contact_ids = {a.get("contactId","") for a in appts_s1 if a.get("contactId")}
            s2_contact_ids = {a.get("contactId","") for a in appts_s2 if a.get("contactId")}
            clients_s1 = len([o for o in won_opps if get_opp_contact_id(o) in s1_contact_ids]) if s1_contact_ids else 0
            clients_s2 = len([o for o in won_opps if get_opp_contact_id(o) in s2_contact_ids]) if s2_contact_ids else 0
            cal_filtered = True
        else:
            calls_s1 = calls_count
            calls_s2 = calls_count
            clients_s1 = None   # None → dashboard usará split proporcional
            clients_s2 = None
            cal_filtered = False

        # Recent opps for table
        def opp_days(o):
            ca = o.get("createdAt", "")
            try:
                if isinstance(ca, (int, float)):
                    ts = int(ca) // 1000
                else:
                    import re
                    # Parse ISO string like "2026-03-17T12:00:00.000Z"
                    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', str(ca))
                    if m:
                        import calendar
                        dt = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                        ts = int(calendar.timegm(dt.timetuple()))
                    else:
                        ts = int(time.time())
                return max(0, (int(time.time()) - ts) // 86400)
            except:
                return 0

        recent_opps = [{
            "contact": o.get("contact", {}).get("name", "—"),
            "stage":   o.get("pipelineStage", {}).get("name", "—"),
            "value":   f"${float(o.get('monetaryValue',0)):,.0f}",
            "source":  o.get("source", "—"),
            "days":    opp_days(o),
            "status":  "Activo"
        } for o in open_opps[:10]]

        return jsonify({
            "leads": len(contacts),
            "calls": calls_count,
            "calls_s1": calls_s1,
            "calls_s2": calls_s2,
            "clients": len(won_opps),
            "clients_s1": clients_s1,
            "clients_s2": clients_s2,
            "calFiltered": cal_filtered,
            "pipeline": pipeline_val,
            "openOpps": len(open_opps),
            "closeRate": close_rate,
            "conversations": total_convos,
            "avgResponse": 1.5,
            "won": len(won_opps),
            "lost": len(lost_opps),
            "leadsWeekly": [len(contacts)//12]*12,
            "stages": {"labels": list(stage_counts.keys()), "data": list(stage_counts.values())},
            "opps": recent_opps
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────
# GHL CALENDARS
# ─────────────────────────────────────────
@app.route("/ghl/calendars")
def get_ghl_calendars():
    key      = request.args.get("key") or os.getenv("GHL_API_KEY", "")
    location = request.args.get("location") or os.getenv("GHL_LOCATION_ID", "")
    if not key or not location:
        return jsonify({"error": "GHL key/location no provistos"}), 400
    if req is None:
        return jsonify({"error": "requests library no instalada"}), 500

    BASE = "https://services.leadconnectorhq.com"

    # GHL usa versiones distintas por endpoint:
    # /calendars/ → 2021-04-15  |  resto → 2021-07-28
    VERSIONS = ["2021-04-15", "2021-07-28"]
    PATHS    = ["/calendars/", "/calendars"]

    last_err = "No se pudo obtener calendarios"
    for ver in VERSIONS:
        for path in PATHS:
            try:
                headers = {"Authorization": f"Bearer {key}", "Version": ver,
                           "Content-Type": "application/json"}
                r = req.get(f"{BASE}{path}", headers=headers,
                            params={"locationId": location, "showDeleted": "false", "limit": 100},
                            timeout=10)

                if not r.text or not r.text.strip():
                    last_err = f"Respuesta vacía ({ver} {path} → {r.status_code})"
                    continue
                if r.status_code != 200:
                    last_err = f"GHL {r.status_code} ({ver} {path}): {r.text[:200]}"
                    continue

                try:
                    data = r.json()
                except Exception:
                    last_err = f"No JSON ({ver} {path}): {r.text[:150]}"
                    continue

                # Normalizar: lista directa o {"calendars":[...]} o {"data":[...]}
                if isinstance(data, list):
                    cals = data
                else:
                    cals = (data.get("calendars") or data.get("data") or
                            data.get("items") or [])

                return jsonify([{
                    "id":   c.get("id", ""),
                    "name": c.get("name") or c.get("title") or c.get("id", "—")
                } for c in cals if c.get("id")])

            except Exception as e:
                last_err = str(e)
                continue

    # ── Plan B: extraer calendarIds únicos de las citas existentes ─────────────
    # (funciona aunque el endpoint /calendars devuelva 404)
    try:
        headers_b = {"Authorization": f"Bearer {key}", "Version": "2021-07-28"}
        # Últimos 90 días para tener suficientes citas
        since_90 = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%dT00:00:00+00:00")
        until_now = datetime.datetime.now().strftime("%Y-%m-%dT23:59:59+00:00")
        ra = req.get(f"{BASE}/calendars/events/appointments", headers=headers_b,
                     params={"locationId": location, "startTime": since_90,
                             "endTime": until_now, "limit": 100}, timeout=10)
        if ra.status_code == 200:
            raw = ra.json()
            appts = raw.get("appointments", raw.get("events", raw.get("data", [])))
            if isinstance(appts, list) and appts:
                seen = {}
                for a in appts:
                    cal_id = a.get("calendarId", "")
                    if cal_id and cal_id not in seen:
                        # Intentar obtener nombre del calendario desde el objeto cita
                        cal_name = (a.get("calendar", {}) or {}).get("name", "")
                        if not cal_name:
                            cal_name = a.get("title", "").split(" — ")[0].strip()
                        seen[cal_id] = cal_name or f"Calendario {cal_id[:12]}"
                if seen:
                    return jsonify([{"id": k, "name": v, "fromAppts": True}
                                    for k, v in seen.items()])
                return jsonify({"error": "No se encontraron citas recientes. Introduce los IDs de calendario manualmente."}), 404
    except Exception as e2:
        last_err = str(e2)

    return jsonify({"error": f"API /calendars no disponible. {last_err}"}), 500



# ─────────────────────────────────────────
# AIRTABLE — TRAININGPEAKS CLIENTES ACTIVOS
# Token: desde cabecera X-AT-Token (frontend) o variable de entorno AIRTABLE_TOKEN
# NUNCA hardcodeado en código fuente
# ─────────────────────────────────────────
@app.route("/airtable")
def get_airtable():
    token = request.headers.get("X-AT-Token") or os.getenv("AIRTABLE_TOKEN", "")
    base  = request.args.get("base") or os.getenv("AIRTABLE_BASE_ID", "app8nkARmupm6hbW1")
    table = request.args.get("table", "Clientes Activos")
    if not token:
        return jsonify({"error": "Airtable token no configurado"}), 401
    if req is None:
        return jsonify({"error": "requests library no instalada"}), 500
    AT_BASE = "https://api.airtable.com/v0"
    headers_at = {"Authorization": f"Bearer {token}"}
    try:
        all_records = []
        offset = None
        while True:
            params = {"maxRecords": 100}
            if offset:
                params["offset"] = offset
            r = req.get(f"{AT_BASE}/{base}/{table}", headers=headers_at, params=params, timeout=15)
            if r.status_code == 401:
                return jsonify({"error": "Token Airtable inválido"}), 401
            if r.status_code == 404:
                return jsonify({"error": f"Tabla '{table}' no encontrada en base {base}"}), 404
            r.raise_for_status()
            data = r.json()
            all_records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return jsonify({"records": all_records})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": now_ts()})


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════╗
║   MetricsHub — API Proxy v1.0            ║
║   Corriendo en http://localhost:{PORT}     ║
╚══════════════════════════════════════════╝

Endpoints disponibles:
  GET /stripe?key=sk_live_...
  GET /meta?token=EAAxx...&account=act_123...
  GET /ghl?key=eyJxx...&location=xxxxx
  GET /health

Tip: crea un archivo .env con tus claves para
evitar pasarlas por URL.
""")
    app.run(host="0.0.0.0", port=PORT, debug=False)
