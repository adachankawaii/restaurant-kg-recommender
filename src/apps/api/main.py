from __future__ import annotations

import html
import json

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from apps.api.deps import get_settings
from apps.api.routers import admin, feedback, health, recommend, restaurants

app = FastAPI(title="Production Restaurant Recommender")
app.include_router(health.router)
app.include_router(recommend.router)
app.include_router(restaurants.router)
app.include_router(feedback.router)
app.include_router(admin.router)


@app.middleware("http")
async def no_cache_html_pages(request, call_next):
    response = await call_next(request)
    if request.url.path in {"/", "/monitoring"}:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.get("/monitoring", response_class=HTMLResponse)
def monitoring_ui():
    settings = get_settings()
    active_graph = ""
    if (settings.paths.kg_root / "ACTIVE_VERSION").exists():
        active_graph = (settings.paths.kg_root / "ACTIVE_VERSION").read_text(encoding="utf-8").strip()

    event_dirs = [path for path in settings.paths.user_events_root.iterdir() if path.is_dir()]
    processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
    rgcn_dirs = [path for path in settings.paths.rgcn_root.iterdir() if path.is_dir()]

    payload = {
        "mode": settings.mode,
        "active_graph_version": active_graph,
        "processed_runs": len(processed_dirs),
        "event_days": len(event_dirs),
        "rgcn_snapshots": len(rgcn_dirs),
        "data_lake_root": str(settings.paths.data_lake_root),
        "user_event_log_root": str(settings.paths.user_events_root),
        "user_event_log_pattern": str(settings.paths.user_events_root / "YYYY-MM-DD" / "events.jsonl"),
    }
    payload_json = html.escape(json.dumps(payload, ensure_ascii=False, indent=2))

    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>System Monitoring</title>
        <style>
          :root {{
            --bg: #f6f7fb;
            --ink: #14213d;
            --muted: #64748b;
            --card: rgba(255,255,255,.86);
            --line: #e2e8f0;
            --accent: #0f766e;
            --accent-soft: #ccfbf1;
            --shadow: 0 24px 70px rgba(15, 23, 42, .10);
          }}
          * {{ box-sizing: border-box; }}
          body {{
            margin: 0;
            min-height: 100vh;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(circle at top left, rgba(20,184,166,.20), transparent 34rem),
              radial-gradient(circle at top right, rgba(59,130,246,.13), transparent 32rem),
              var(--bg);
          }}
          .shell {{ max-width: 1120px; margin: 0 auto; padding: 36px 20px; }}
          .topbar {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 24px; }}
          .brand {{ display: flex; gap: 12px; align-items: center; }}
          .logo {{ width: 44px; height: 44px; display: grid; place-items: center; border-radius: 15px; background: linear-gradient(135deg, #0f766e, #14b8a6); color: white; font-size: 22px; box-shadow: var(--shadow); }}
          h1 {{ font-size: clamp(28px, 5vw, 44px); margin: 0; letter-spacing: -.04em; }}
          .subtitle {{ margin: 6px 0 0; color: var(--muted); }}
          .status-pill {{ padding: 9px 13px; border-radius: 999px; border: 1px solid rgba(15,118,110,.24); background: rgba(204,251,241,.65); color: #115e59; font-weight: 700; }}
          .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 20px 0; }}
          .metric, .panel {{ background: var(--card); border: 1px solid rgba(226,232,240,.86); border-radius: 24px; box-shadow: var(--shadow); backdrop-filter: blur(16px); }}
          .metric {{ padding: 18px; }}
          .metric-label {{ color: var(--muted); font-size: 13px; }}
          .metric-value {{ margin-top: 8px; font-size: 30px; font-weight: 800; letter-spacing: -.03em; }}
          .panel {{ padding: 22px; margin-top: 16px; }}
          pre {{ margin: 0; background: #0f172a; color: #d1fae5; padding: 18px; border-radius: 18px; overflow: auto; line-height: 1.55; }}
          @media (max-width: 800px) {{ .topbar {{ align-items: flex-start; flex-direction: column; }} .grid {{ grid-template-columns: 1fr 1fr; }} }}
          @media (max-width: 520px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        </style>
      </head>
      <body>
        <main class="shell">
          <div class="topbar">
            <div class="brand">
              <div class="logo">◈</div>
              <div>
                <h1>System Monitoring</h1>
                <p class="subtitle">Production pipeline status, graph version and local lake paths.</p>
              </div>
            </div>
            <div class="status-pill">Mode: {html.escape(str(settings.mode))}</div>
          </div>

          <section class="grid">
            <div class="metric"><div class="metric-label">Active graph</div><div class="metric-value">{html.escape(active_graph or "N/A")}</div></div>
            <div class="metric"><div class="metric-label">Processed runs</div><div class="metric-value">{len(processed_dirs)}</div></div>
            <div class="metric"><div class="metric-label">Event days</div><div class="metric-value">{len(event_dirs)}</div></div>
            <div class="metric"><div class="metric-label">R-GCN snapshots</div><div class="metric-value">{len(rgcn_dirs)}</div></div>
          </section>

          <section class="panel">
            <pre>{payload_json}</pre>
          </section>
        </main>
      </body>
    </html>
    """


@app.get("/", response_class=HTMLResponse)
def user_ui():
    return """
    <!doctype html>
    <html lang="vi">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Restaurant Recommender</title>
        <style>
          :root {
            --bg: #f6f7fb;
            --ink: #111827;
            --muted: #64748b;
            --card: rgba(255,255,255,.88);
            --line: #e2e8f0;
            --accent: #0f766e;
            --accent-2: #14b8a6;
            --accent-soft: #ccfbf1;
            --danger: #dc2626;
            --warning: #f59e0b;
            --shadow: 0 24px 70px rgba(15, 23, 42, .10);
            --shadow-soft: 0 12px 30px rgba(15, 23, 42, .08);
            --radius: 26px;
          }
          * { box-sizing: border-box; }
          body {
            margin: 0;
            min-height: 100vh;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(circle at 8% 8%, rgba(20,184,166,.20), transparent 28rem),
              radial-gradient(circle at 90% 0%, rgba(59,130,246,.15), transparent 26rem),
              linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
          }
          button, textarea, select, input { font: inherit; }
          button { border: 0; cursor: pointer; }
          .shell { width: min(1180px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 44px; }
          .nav {
            display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 22px;
          }
          .brand { display: flex; align-items: center; gap: 12px; font-weight: 800; letter-spacing: -.02em; }
          .brand-mark {
            width: 42px; height: 42px; border-radius: 15px; display: grid; place-items: center;
            background: linear-gradient(135deg, var(--accent), var(--accent-2)); color: white;
            box-shadow: 0 14px 34px rgba(15,118,110,.26); font-size: 21px;
          }
          .nav-links { display: flex; gap: 10px; align-items: center; }
          .nav-link {
            color: #334155; text-decoration: none; padding: 10px 13px; border-radius: 999px;
            border: 1px solid rgba(226,232,240,.9); background: rgba(255,255,255,.65);
          }
          .hero {
            position: relative; overflow: hidden; min-height: 300px; padding: clamp(26px, 5vw, 52px);
            border-radius: 34px; background:
              linear-gradient(135deg, rgba(15,118,110,.96), rgba(20,184,166,.84)),
              radial-gradient(circle at 85% 10%, rgba(255,255,255,.33), transparent 15rem);
            color: white; box-shadow: var(--shadow);
          }
          .hero::after {
            content: ""; position: absolute; inset: auto -60px -120px auto; width: 360px; height: 360px;
            border-radius: 50%; background: rgba(255,255,255,.16);
          }
          .hero-content { position: relative; z-index: 1; max-width: 760px; }
          .eyebrow {
            display: inline-flex; align-items: center; gap: 8px; margin-bottom: 16px; padding: 8px 12px;
            border: 1px solid rgba(255,255,255,.24); border-radius: 999px; background: rgba(255,255,255,.13);
            backdrop-filter: blur(10px); font-weight: 700; font-size: 14px;
          }
          h1 { margin: 0; font-size: clamp(36px, 7vw, 68px); line-height: .96; letter-spacing: -.06em; }
          .hero p { margin: 18px 0 0; color: rgba(255,255,255,.88); line-height: 1.75; font-size: 17px; }
          .layout { display: grid; grid-template-columns: minmax(0, 1fr); gap: 22px; align-items: start; margin-top: 22px; }
          .panel {
            background: var(--card); border: 1px solid rgba(226,232,240,.86); border-radius: var(--radius);
            box-shadow: var(--shadow-soft); backdrop-filter: blur(16px); overflow: hidden;
          }
          .panel-head { padding: 20px 22px 0; display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
          .panel-title { margin: 0; font-size: 20px; letter-spacing: -.03em; }
          .panel-sub { margin: 6px 0 0; color: var(--muted); font-size: 14px; line-height: 1.5; }
          .panel-body { padding: 22px; }
          .mode-toggle {
            display: inline-grid; grid-template-columns: 1fr 1fr; gap: 4px; padding: 4px; margin-bottom: 16px;
            border: 1px solid #dbe3ea; border-radius: 16px; background: #f8fafc;
          }
          .mode-btn {
            min-height: 40px; padding: 0 14px; border-radius: 12px; color: #475569; background: transparent;
            font-weight: 850;
          }
          .mode-btn.active { color: white; background: linear-gradient(135deg, var(--accent), var(--accent-2)); }
          .mode-panel[hidden] { display: none; }
          .query-box {
            border: 1px solid #dbe3ea; background: white; border-radius: 24px; padding: 14px; box-shadow: inset 0 1px 0 rgba(15,23,42,.03);
          }
          textarea {
            width: 100%; min-height: 132px; resize: vertical; border: 0; outline: 0; background: transparent;
            color: var(--ink); line-height: 1.6;
          }
          .toolbar { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 16px; align-items: center; }
          .field { display: grid; gap: 6px; flex: 1 1 150px; }
          label { font-size: 13px; font-weight: 800; color: #334155; }
          select, input {
            width: 100%; height: 44px; border-radius: 15px; border: 1px solid #dbe3ea; background: #fff;
            padding: 0 12px; color: var(--ink); outline: 0;
          }
          .hard-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
          .aspect-grid { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }
          .check-pill {
            display: inline-flex; align-items: center; gap: 7px; min-height: 38px; padding: 0 11px;
            border: 1px solid #dbe3ea; border-radius: 999px; background: white; color: #334155; font-size: 13px; font-weight: 800;
          }
          .check-pill input { width: 16px; height: 16px; padding: 0; }
          .primary {
            min-height: 48px; padding: 0 20px; border-radius: 17px; color: white; font-weight: 800;
            background: linear-gradient(135deg, var(--accent), var(--accent-2));
            box-shadow: 0 16px 34px rgba(15,118,110,.24); transition: transform .15s ease, box-shadow .15s ease;
          }
          .primary:hover { transform: translateY(-1px); box-shadow: 0 20px 42px rgba(15,118,110,.30); }
          .primary:disabled { opacity: .65; cursor: wait; transform: none; }
          .secondary {
            min-height: 48px; padding: 0 16px; border-radius: 17px; color: #115e59; font-weight: 800;
            background: #ecfdf5; border: 1px solid rgba(15,118,110,.20);
          }
          .location-fields { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-top: 10px; }
          .location-fields input { width: 150px; height: 40px; border-radius: 12px; }
          .chips { display: flex; flex-wrap: wrap; gap: 9px; margin-top: 14px; }
          .chip {
            padding: 9px 12px; border-radius: 999px; border: 1px solid rgba(15,118,110,.16);
            color: #115e59; background: rgba(204,251,241,.55); font-size: 13px; font-weight: 700;
          }
          .chip:hover { background: rgba(204,251,241,.9); }
          .results { display: grid; gap: 14px; }
          .result-card {
            position: relative; display: grid; gap: 10px; padding: 17px; border: 1px solid #e2e8f0; border-radius: 22px;
            background: rgba(255,255,255,.94); cursor: pointer; transition: transform .15s ease, box-shadow .15s ease, border-color .15s ease;
          }
          .result-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-soft); border-color: rgba(20,184,166,.40); }
          .result-top { display: flex; gap: 13px; align-items: flex-start; justify-content: space-between; }
          .rank {
            flex: 0 0 auto; width: 36px; height: 36px; border-radius: 14px; display: grid; place-items: center;
            background: #f0fdfa; color: #0f766e; font-weight: 900;
          }
          .result-main { min-width: 0; flex: 1; }
          .restaurant-name { margin: 0; font-size: 18px; letter-spacing: -.03em; }
          .meta { color: var(--muted); font-size: 13px; line-height: 1.5; }
          .score-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
          .pill {
            display: inline-flex; align-items: center; gap: 5px; padding: 6px 9px; border-radius: 999px;
            background: #f8fafc; border: 1px solid #e2e8f0; color: #475569; font-size: 12px; font-weight: 700;
          }
          .matched { display: flex; gap: 8px; flex-wrap: wrap; }
          .badge {
            display: inline-flex; padding: 7px 10px; border-radius: 999px; background: var(--accent-soft);
            color: #115e59; font-size: 12px; font-weight: 800;
          }
          .evidence { display: grid; gap: 6px; padding-top: 4px; }
          .evidence-line { color: #64748b; font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
          .empty, .error, .loading {
            border: 1px dashed #cbd5e1; border-radius: 22px; padding: 24px; text-align: center; color: var(--muted); background: rgba(248,250,252,.75);
          }
          .error { color: var(--danger); border-color: rgba(220,38,38,.28); background: #fff5f5; }
          .detail-panel { display: none; position: sticky; top: 18px; }
          .detail-card { padding: 22px; }
          .detail-hero {
            border-radius: 22px; padding: 22px; background: linear-gradient(135deg, #0f172a, #115e59); color: white; margin-bottom: 16px;
          }
          .detail-hero h2 { margin: 0; font-size: 25px; letter-spacing: -.04em; }
          .detail-list { display: grid; gap: 12px; }
          .detail-row { padding: 13px; border-radius: 17px; background: #f8fafc; border: 1px solid #e2e8f0; }
          .detail-label { color: var(--muted); font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: .06em; }
          .detail-value { margin-top: 4px; color: var(--ink); }
          .inline-detail {
            display: none; margin-top: 12px; padding-top: 14px; border-top: 1px solid #e2e8f0;
          }
          .result-card.is-open .inline-detail { display: block; }
          .inline-detail-grid { display: grid; gap: 12px; }
          .inline-detail-section { display: grid; gap: 8px; }
          .inline-detail-title { margin: 0; color: #334155; font-size: 13px; font-weight: 900; text-transform: uppercase; letter-spacing: .08em; }
          .menu-list { display: flex; gap: 8px; flex-wrap: wrap; }
          .detail-text { color: #475569; line-height: 1.55; font-size: 14px; }
          .footer-note { color: var(--muted); font-size: 12px; text-align: center; padding: 18px 0 0; }
          @media (max-width: 900px) {
            .layout { grid-template-columns: 1fr; }
            .hard-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .detail-panel { position: static; }
            .nav { flex-direction: column; align-items: flex-start; }
          }
          @media (max-width: 560px) {
            .shell { width: min(100% - 20px, 1180px); padding-top: 14px; }
            .hero { border-radius: 26px; }
            .toolbar { display: grid; grid-template-columns: 1fr; }
            .hard-grid { grid-template-columns: 1fr; }
            .primary { width: 100%; }
          }
        </style>
      </head>
      <body>
        <main class="shell">
          <nav class="nav">
            <div class="brand"><div class="brand-mark">🍜</div><span>BK Food Graph</span></div>
            <div class="nav-links">
              <a class="nav-link" href="/monitoring">Monitoring</a>
              <a class="nav-link" href="/docs">API Docs</a>
            </div>
          </nav>

          <section class="hero">
            <div class="hero-content">
              <h1>Gợi ý quán ăn quanh Bách Khoa</h1>
              <p></p>
            </div>
          </section>

          <section class="layout">
            <div class="left-col">
              <section class="panel">
                <div class="panel-head">
                  <div>
                    <h2 class="panel-title">Tìm kiếm thông minh</h2>
                    <p class="panel-sub"></p>
                  </div>
                </div>
                <div class="panel-body">
                  <div class="mode-toggle" role="tablist" aria-label="Recommendation mode">
                    <button id="nlModeButton" class="mode-btn active" type="button" onclick="setMode('nl')">NL</button>
                    <button id="hardModeButton" class="mode-btn" type="button" onclick="setMode('hard')">Hard feature</button>
                  </div>

                  <div id="nlPanel" class="mode-panel">
                    <div class="query-box">
                      <textarea id="query" placeholder="Nhập nhu cầu ăn uống của bạn"></textarea>
                    </div>
                  </div>

                  <div id="hardPanel" class="mode-panel" hidden>
                    <div class="hard-grid">
                      <div class="field">
                        <label for="hardFood">Món</label>
                        <select id="hardFood">
                          <option value="com ga">Cơm gà</option>
                          <option value="pho">Phở</option>
                          <option value="bun">Bún</option>
                          <option value="bun cha">Bún chả</option>
                          <option value="banh cuon">Bánh cuốn</option>
                          <option value="com rang">Cơm rang</option>
                          <option value="ga ran">Gà rán</option>
                          <option value="do uong">Đồ uống</option>
                        </select>
                      </div>
                      <div class="field">
                        <label for="hardArea">Khu vực</label>
                        <select id="hardArea">
                          <option value="bach khoa hai ba trung" data-lat="21.005118" data-lng="105.845592">Bách Khoa</option>
                          <option value="dai hoc bach khoa hai ba trung" data-lat="21.006111" data-lng="105.843889">Đại học Bách Khoa</option>
                          <option value="cong tran dai nghia" data-lat="21.006563" data-lng="105.842980">Cổng Trần Đại Nghĩa</option>
                          <option value="cong parabol" data-lat="21.005343" data-lng="105.843892">Cổng Parabol</option>
                          <option value="le thanh nghi" data-lat="21.003774" data-lng="105.845069">Lê Thanh Nghị</option>
                          <option value="ta quang buu" data-lat="21.004233" data-lng="105.846766">Tạ Quang Bửu</option>
                        </select>
                      </div>
                      <div class="field">
                        <label for="hardPrice">Giá</label>
                        <select id="hardPrice">
                          <option value="under_50k" data-max="50000">Dưới 50k</option>
                          <option value="50k_100k" data-max="100000">50k-100k</option>
                          <option value="over_100k" data-max="150000">Trên 100k</option>
                        </select>
                      </div>
                      <div class="field">
                        <label for="hardTime">Thời điểm</label>
                        <select id="hardTime">
                          <option value="breakfast">Sáng</option>
                          <option value="lunch">Trưa</option>
                          <option value="afternoon">Chiều</option>
                          <option value="dinner">Tối</option>
                        </select>
                      </div>
                    </div>
                    <div class="hard-grid" style="margin-top:12px;">
                      <div class="field">
                        <label for="hardDistance">Bán kính</label>
                        <select id="hardDistance">
                          <option value="700">700m</option>
                          <option value="1000">1km</option>
                          <option value="1500">1.5km</option>
                          <option value="2000">2km</option>
                        </select>
                      </div>
                    </div>
                    <div class="aspect-grid">
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="price" checked /> Giá</label>
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="food_quality" checked /> Chất lượng món</label>
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="service" /> Phục vụ</label>
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="cleanliness" /> Sạch sẽ</label>
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="speed" /> Nhanh</label>
                      <label class="check-pill"><input type="checkbox" name="hardAspect" value="location" /> Gần</label>
                    </div>
                  </div>

                  <div class="toolbar">
                    <button id="runButton" class="primary" onclick="runRecommend()">Gợi ý ngay</button>
                    <button id="locationButton" class="secondary" onclick="useCurrentLocation()">Dùng vị trí của tôi</button>
                    <button class="secondary" onclick="useBachKhoaLocation()">Dùng vị trí Bách Khoa</button>
                    <span id="locationStatus" class="meta"></span>
                  </div>
                  <div class="location-fields">
                    <input id="manualLat" type="number" step="any" placeholder="Lat" />
                    <input id="manualLng" type="number" step="any" placeholder="Lng" />
                    <button class="chip" type="button" onclick="useManualLocation()">Áp dụng tọa độ</button>
                  </div>
                </div>
              </section>

              <section class="panel" style="margin-top:22px;">
                <div class="panel-head">
                  <div>
                    <h2 class="panel-title">Kết quả gợi ý</h2>
                    <p id="resultSummary" class="panel-sub">Kết quả sẽ xuất hiện ở đây.</p>
                  </div>
                </div>
                <div class="panel-body">
                  <div id="results" class="results">
                    <div class="empty">Chưa có kết quả. Hãy nhập truy vấn và bấm “Gợi ý ngay”.</div>
                  </div>
                </div>
              </section>
            </div>
          </section>
          <div class="footer-note">Chi tiết quán chỉ mở khi bấm vào kết quả.</div>
        </main>

        <script>
          let currentSessionId = null;
          let currentLocation = null;
          let currentMode = 'nl';
          const bachKhoaLocation = { lat: 21.005118, lng: 105.845592 };

          function escapeHtml(value) {
            return String(value ?? '')
              .replaceAll('&', '&amp;')
              .replaceAll('<', '&lt;')
              .replaceAll('>', '&gt;')
              .replaceAll('"', '&quot;')
              .replaceAll("'", '&#039;');
          }

          function formatPrice(value) {
            if (value === null || value === undefined || value === '') return '';
            const number = Number(value);
            if (Number.isFinite(number)) return number.toLocaleString('vi-VN') + 'đ';
            return escapeHtml(value);
          }

          function setLoading(isLoading) {
            const button = document.getElementById('runButton');
            button.disabled = isLoading;
            button.textContent = isLoading ? 'Đang tìm...' : 'Gợi ý ngay';
          }

          function resetResults() {
            document.getElementById('resultSummary').textContent = 'Kết quả sẽ xuất hiện ở đây.';
            document.getElementById('results').innerHTML = '<div class="empty">Chưa có kết quả. Hãy chọn nhu cầu và bấm “Gợi ý ngay”.</div>';
          }

          function setMode(mode) {
            currentMode = mode;
            document.getElementById('nlPanel').hidden = mode !== 'nl';
            document.getElementById('hardPanel').hidden = mode !== 'hard';
            document.getElementById('nlModeButton').classList.toggle('active', mode === 'nl');
            document.getElementById('hardModeButton').classList.toggle('active', mode === 'hard');
            resetResults();
          }

          function setCurrentLocation(lat, lng, label) {
            currentLocation = { lat, lng };
            document.getElementById('manualLat').value = lat;
            document.getElementById('manualLng').value = lng;
            document.getElementById('locationStatus').textContent = label;
          }

          function useBachKhoaLocation() {
            setCurrentLocation(bachKhoaLocation.lat, bachKhoaLocation.lng, 'Đang dùng vị trí Bách Khoa.');
          }

          function useManualLocation() {
            const lat = Number(document.getElementById('manualLat').value);
            const lng = Number(document.getElementById('manualLng').value);
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
              document.getElementById('locationStatus').textContent = 'Tọa độ không hợp lệ.';
              return;
            }
            setCurrentLocation(lat, lng, 'Đã dùng tọa độ nhập tay.');
          }

          function useCurrentLocation() {
            const status = document.getElementById('locationStatus');
            if (!navigator.geolocation) {
              status.textContent = 'Trình duyệt không hỗ trợ lấy vị trí. Có thể nhập tọa độ hoặc dùng vị trí Bách Khoa.';
              return;
            }
            status.textContent = 'Đang lấy vị trí...';
            navigator.geolocation.getCurrentPosition(
              (position) => {
                setCurrentLocation(position.coords.latitude, position.coords.longitude, 'Đã dùng vị trí hiện tại.');
              },
              (error) => {
                const reason = error.code === 1 ? 'Trình duyệt từ chối quyền vị trí.' : 'Không lấy được vị trí.';
                status.textContent = reason + ' Có thể nhập tọa độ hoặc dùng vị trí Bách Khoa.';
              },
              { enableHighAccuracy: true, timeout: 10000, maximumAge: 300000 }
            );
          }

          function selectedOption(selectId) {
            const select = document.getElementById(selectId);
            return select.options[select.selectedIndex];
          }

          function selectedHardAspects() {
            return Array.from(document.querySelectorAll('input[name="hardAspect"]:checked')).map(item => item.value);
          }

          function buildHardFeatureRequest() {
            const food = document.getElementById('hardFood').value;
            const termAliases = {
              'com ga': ['com ga'],
              'pho': ['pho'],
              'bun': ['bun'],
              'bun cha': ['bun cha'],
              'banh cuon': ['banh cuon'],
              'com rang': ['com rang'],
              'ga ran': ['ga ran'],
              'do uong': ['thuc uong', 'giai khat']
            };
            const areaOption = selectedOption('hardArea');
            const priceOption = selectedOption('hardPrice');
            const distance = Number(document.getElementById('hardDistance').value || 1500);
            const queryLat = Number(areaOption.dataset.lat);
            const queryLng = Number(areaOption.dataset.lng);
            const hardFeatures = {
              food,
              terms: termAliases[food] || [food],
              location: areaOption.value,
              area_id: areaOption.value,
              desired_price_range_id: priceOption.value,
              max_price: Number(priceOption.dataset.max),
              time_slot_id: document.getElementById('hardTime').value,
              preferred_aspects: selectedHardAspects(),
              distance_tolerance_m: distance,
              query_lat: queryLat,
              query_lng: queryLng
            };
            const query = `${food} quanh ${areaOption.textContent.trim()} ${priceOption.textContent.trim()}`;
            return {
              query,
              mode: 'hard_feature',
              algorithm: 'rgcn',
              top_k: 5,
              hard_features: hardFeatures,
              user_lat: queryLat,
              user_lng: queryLng,
              distance_tolerance_m: distance
            };
          }

          async function runRecommend() {
            const results = document.getElementById('results');
            const summary = document.getElementById('resultSummary');
            let body;

            if (currentMode === 'hard') {
              body = buildHardFeatureRequest();
            } else {
              const query = document.getElementById('query').value.trim();
              if (!query) {
                results.innerHTML = '<div class="error">Vui lòng nhập nội dung tìm kiếm.</div>';
                return;
              }
              body = { query, mode: 'nl', algorithm: 'hybrid', top_k: 5 };
              if (currentLocation) {
                body.user_lat = currentLocation.lat;
                body.user_lng = currentLocation.lng;
                body.distance_tolerance_m = 1500;
              }
            }

            setLoading(true);
            results.innerHTML = currentMode === 'hard'
              ? '<div class="loading">Đang xếp hạng bằng R-GCN...</div>'
              : '<div class="loading">Đang truy vấn GraphRAG và xếp hạng kết quả...</div>';
            summary.textContent = 'Đang xử lý truy vấn...';

            try {
              const res = await fetch('/recommend', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
              });
              if (!res.ok) throw new Error('Request failed: ' + res.status);

              const data = await res.json();
              currentSessionId = data.session_id;
              renderResults(data.results || []);
            } catch (error) {
              console.error(error);
              summary.textContent = 'Có lỗi khi gọi API.';
              results.innerHTML = '<div class="error">Không gọi được /recommend. Hãy kiểm tra API server, dữ liệu hoặc log backend.</div>';
            } finally {
              setLoading(false);
            }
          }

          function renderResults(items) {
            const container = document.getElementById('results');
            const summary = document.getElementById('resultSummary');
            container.innerHTML = '';

            if (!items.length) {
              summary.textContent = 'Không tìm thấy kết quả phù hợp.';
              container.innerHTML = '<div class="empty">Không có quán phù hợp với truy vấn hiện tại.</div>';
              return;
            }

            summary.textContent = `Tìm thấy ${items.length} kết quả phù hợp. Bấm vào một quán để xem chi tiết.`;

            items.forEach((item, index) => {
              const rating = item.rating ?? '';
              const distance = item.distance_km ?? '';

              const el = document.createElement('article');
              el.className = 'result-card';
              el.onclick = () => openDetail(item, index + 1, el);
              el.innerHTML = `
                <div class="result-top">
                  <div class="result-main">
                    <h3 class="restaurant-name">${escapeHtml(item.name || 'Restaurant')}</h3>
                    <div class="score-row" style="margin-top:8px;">
                      <span class="pill">Rating: ${escapeHtml(rating || 'N/A')}</span>
                      <span class="pill">Khoảng cách: ${distance !== null && distance !== undefined && distance !== '' ? escapeHtml(distance) + ' km' : 'N/A'}</span>
                    </div>
                  </div>
                </div>
                <div class="inline-detail" data-detail-for="${escapeHtml(item.restaurant_id)}"></div>
              `;
              container.appendChild(el);
            });
          }

          async function openDetail(resultItem, rankPosition, cardEl) {
            const restaurantId = resultItem.restaurant_id;
            if (!restaurantId) return;
            const detail = cardEl.querySelector('.inline-detail');
            const alreadyOpen = cardEl.classList.contains('is-open');
            document.querySelectorAll('.result-card.is-open').forEach(card => {
              if (card !== cardEl) card.classList.remove('is-open');
            });
            cardEl.classList.toggle('is-open', !alreadyOpen);
            if (alreadyOpen) return;

            detail.innerHTML = '<div class="loading">Đang tải chi tiết...</div>';

            try {
              const params = new URLSearchParams({
                session_id: currentSessionId || '',
                rank_position: String(rankPosition)
              });
              const res = await fetch(`/restaurants/${restaurantId}?${params.toString()}`);
              if (!res.ok) throw new Error('Detail request failed: ' + res.status);
              const item = await res.json();
              const matchedItems = item.menu_items && item.menu_items.length ? item.menu_items : (resultItem.matched_items || []);
              const matched = matchedItems.slice(0, 20).map(mi => {
                const price = mi.price ? ' - ' + formatPrice(mi.price) : '';
                return `<span class="badge">${escapeHtml(mi.name)}${price}</span>`;
              }).join('');
              const extractedEntities = item.extracted_entities && item.extracted_entities.length ? item.extracted_entities : (resultItem.extracted_entities || []);
              const reviewTerms = extractedEntities.slice(0, 20).map(entity => {
                const type = entity.type ? ` - ${escapeHtml(entity.type)}` : '';
                return `<span class="badge">${escapeHtml(entity.name)}${type}</span>`;
              }).join('');
              const comments = (item.comments || []).slice(0, 5).map(comment => {
                return `<div class="detail-text">"${escapeHtml(comment)}"</div>`;
              }).join('');
              const reviewEvidence = (item.review_evidence || []).slice(0, 5).map(review => {
                const text = review.feedback || review.chunk_text || '';
                return `<div class="detail-text">"${escapeHtml(text)}"</div>`;
              }).join('');

              detail.innerHTML = `
                <div class="inline-detail-grid">
                  <div class="inline-detail-section">
                    <p class="inline-detail-title">Thông tin quán</p>
                    <div class="detail-text">${escapeHtml(item.address || 'Chưa có địa chỉ')}</div>
                    <div class="detail-text">Reviews: ${escapeHtml(item.review_count || 'N/A')} · Giờ mở cửa: ${escapeHtml(item.opening_hours || 'N/A')}</div>
                  </div>
                  <div class="inline-detail-section">
                    <p class="inline-detail-title">Menu từ nguồn</p>
                    <div class="menu-list">${matched || '<span class="meta">Nguồn dữ liệu chưa có menu cho quán này.</span>'}</div>
                  </div>
                  <div class="inline-detail-section">
                    <p class="inline-detail-title">Từ khóa review chính</p>
                    <div class="menu-list">${reviewTerms || '<span class="meta">Nguồn dữ liệu chưa có từ khóa review cho quán này.</span>'}</div>
                  </div>
                  <div class="inline-detail-section">
                    <p class="inline-detail-title">Review gốc</p>
                    <div>${comments || reviewEvidence || '<span class="meta">Nguồn dữ liệu chưa có nội dung review cho quán này.</span>'}</div>
                  </div>
                </div>
              `;
            } catch (error) {
              console.error(error);
              detail.innerHTML = `
                <div class="error">Không tải được chi tiết quán.</div>
              `;
            }
          }

        </script>
      </body>
    </html>
    """
