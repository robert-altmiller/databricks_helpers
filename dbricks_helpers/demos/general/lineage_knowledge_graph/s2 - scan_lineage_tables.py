# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install networkx gravis
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import json, os, shutil
from collections import Counter
import pyspark.sql.functions as F
import networkx as nx
import gravis as gv
import numpy as np

# COMMAND ----------

# DBTITLE 1,User Parameters
# Workspace url
workspace_url = str(spark.conf.get("spark.databricks.workspaceUrl"))

# Catalog and schema for writing lineage data
catalog = "dna_dev"
schema = "raltmil"

# Lineage table for node and edge graph creation
lineage_tables_base = "lineage_tables_base"
lineage_tables_combined = "lineage_tables_combined"
lineage_upstream_jobs = "lineage_upstream_jobs" 

# Output path for index.html node edge graph
output_path = "/Workspace/Users/robert.altmiller@databricks.com/knowledge_graph/index.html" 

# COMMAND ----------

# DBTITLE 1,Python Setup + HTML Header (Version 4)
# ============================================================
# Databricks Lineage Visualization — Unified Pro + Hybrid Auto-Fit
# (Top10, Legend, Summary, Dark, Spacing, Fuzzy Search, Multi-Select)
# + 🧭 Hybrid Auto-Fit (Camera Fit + Dynamic Re-Layout)
# ============================================================

# ---- Config -------------------------------------------------
OUTPUT_PATH = output_path  # Or local path if not on Databricks
DISPLAY_INLINE = True
DEGREE_THRESHOLD = 0
MAX_NODES = 0
ENABLE_TOP10 = True
ENABLE_LEGEND = True
ENABLE_SUMMARY = True
ENABLE_DARK_TOGGLE = True
ENABLE_SPACING = True

# ---- 1) Input: lineage_result_df -> Pandas -----------------
# expects a Spark DataFrame named lineage_result_df with columns:
#   source_table_full_name, target_table_full_name
lineage_pd = spark.read.table(f"{catalog}.{schema}.{lineage_tables_combined}") \
    .select("source_table_full_name", "target_table_full_name") \
    .where("source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL") \
    .distinct() \
    .toPandas()

# ---- 1.5) Build lookup of node types (TABLE / VIEW) --------
type_lookup = {}
type_rows = (
    spark.read.table(f"{catalog}.{schema}.{lineage_tables_combined}")
    .select("source_table_full_name", "target_table_full_name", "source_type", "target_type")
    .distinct()
    .collect()
)

for r in type_rows:
    if r.source_table_full_name:
        type_lookup[r.source_table_full_name] = r.source_type or type_lookup.get(r.source_table_full_name)
    if r.target_table_full_name:
        type_lookup[r.target_table_full_name] = r.target_type or type_lookup.get(r.target_table_full_name)

# ---- 2) Extract catalog.schema -----------------------------
def extract_catalog_schema(value: str) -> str:
    """Extract catalog.schema or path-based prefix."""
    if not value or not isinstance(value, str):
        return "unknown"
    v = value.lower()
    if v.startswith(("abfss:", "dbfs:", "s3:")):
        return v.split(":")[0]
    parts = v.split(".")
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else "unknown"

# ---- 3) Build graph ----------------------------------------
nodes = set(lineage_pd["source_table_full_name"]).union(
    set(lineage_pd["target_table_full_name"])
)
G = nx.DiGraph()
edges = [(u, v) for (u, v) in lineage_pd.itertuples(index=False, name=None) if u and v and u != v]
G.add_edges_from(edges)

# ---- 4) Annotate nodes + compute stats ---------------------
catalog_schema_map = {n: extract_catalog_schema(n) for n in G.nodes()}
node_stats = [(n, len(list(G.predecessors(n))), len(list(G.successors(n)))) for n in G.nodes()]

# Optional scaling: degree filter + cap to top-N
if DEGREE_THRESHOLD > 0 or MAX_NODES > 0:
    deg_map = {n: (u + d) for (n, u, d) in node_stats}
    keep = {n for n, ud in deg_map.items() if ud >= DEGREE_THRESHOLD}
    if MAX_NODES and len(keep) > MAX_NODES:
        keep = set(sorted(keep, key=lambda n: deg_map[n], reverse=True)[:MAX_NODES])
    remove = [n for n in list(G.nodes()) if n not in keep]
    G.remove_nodes_from(remove)
    catalog_schema_map = {n: extract_catalog_schema(n) for n in G.nodes()}
    node_stats = [(n, len(list(G.predecessors(n))), len(list(G.successors(n)))) for n in G.nodes()]

# ---- 5) Top 10 lists ---------------------------------------
top_up         = sorted(node_stats, key=lambda x: x[1], reverse=True)[:10]
top_down       = sorted(node_stats, key=lambda x: x[2], reverse=True)[:10]
view_stats = [x for x in node_stats if type_lookup.get(x[0], "").upper() == "VIEW"]
top_view_up    = sorted(view_stats, key=lambda x: x[1], reverse=True)[:10]
top_view_down  = sorted(view_stats, key=lambda x: x[2], reverse=True)[:10]

# ---- 6) Colors by schema -----------------------------------
schemas = sorted(set(catalog_schema_map.values()))
def schema_color(i, total):
    h = (i * (360.0 / max(1,total))) % 360
    return f"hsl({int(h)},60%,50%)"
color_map = {s: schema_color(i, len(schemas)) for i, s in enumerate(schemas)}

# ---- 7) Schema counts for legend ---------------------------
schema_counts = Counter(catalog_schema_map.values())

# ---- 8) Serialize graph data for D3 ------------------------
def split_name(value: str):
    """Split full name into catalog, schema, and table (best effort)."""
    if not value or not isinstance(value, str):
        return ("unknown", "unknown", value)
    parts = value.split(".")
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    elif len(parts) == 2:
        return parts[0], parts[1], parts[1]
    else:
        return ("unknown", "unknown", value)

# ======================================================
# Load upstream jobs (before node construction)
# ======================================================

jobs_df = spark.read.table(f"{catalog}.{schema}.{lineage_upstream_jobs}") \
    .select("target_table_full_name", "jobs")

jobs_lookup = {
    r["target_table_full_name"]: (
        json.loads(r["jobs"]) if isinstance(r["jobs"], str) else r["jobs"]
    )
    for r in jobs_df.collect()
}

# ============================================================
# Build D3 Nodes (using lineage_tables_columns_data for columns)
# ============================================================

# ✅ Load precomputed column metadata once
cols_df = (
    spark.read.table(f"{catalog}.{schema}.lineage_tables_columns_data")
    .select("full_table_name", "uc_columns")
    .distinct()
)
cols_lookup = {r["full_table_name"]: r["uc_columns"] for r in cols_df.collect()}

# ✅ Load precomputed column metadata once
cols_df = (
    spark.read.table(f"{catalog}.{schema}.lineage_tables_columns_data")
    .select("full_table_name", "uc_columns")
    .distinct()
)
cols_lookup = {r["full_table_name"]: r["uc_columns"] for r in cols_df.collect()}

# ============================================================
# Build D3 Nodes — with CLEAN job normalization
# ============================================================
d3_nodes = []

for n, u, d in node_stats:
    catalog_name, schema_name, table_name = split_name(n)

    # ---- Determine type (VIEW / TABLE / UNKNOWN) ----
    node_type = type_lookup.get(n, "UNKNOWN")
    label_text = f"{table_name} ({node_type.lower()})" if node_type else table_name

    # ---- Use precomputed columns ----
    cols = cols_lookup.get(n, []) or []
    total_cols = len(cols)
    col_text = ", ".join(sorted(cols)) if cols else "—"

    # =================================================
    # ⭐ CLEAN JOB NORMALIZATION ⭐
    # =================================================
    raw_jobs = jobs_lookup.get(n, []) or []
    
    # If JSON string → parse it
    if isinstance(raw_jobs, str):
        try:
            raw_jobs = json.loads(raw_jobs)
        except:
            raw_jobs = []

    normalized_jobs = []

    for j in raw_jobs:
        # Spark Row → dict
        if hasattr(j, "asDict"):
            normalized_jobs.append(j.asDict())

        # Already dict → keep it
        elif isinstance(j, dict):
            normalized_jobs.append(j)

        # List → convert to dict
        elif isinstance(j, list) and len(j) >= 2:
            normalized_jobs.append({
                "job_name": j[0],
                "job_url": j[1]
            })

        # Unexpected type → skip
        else:
            print("⚠️ Unknown job type:", j)

    jobs = normalized_jobs

    # ---- Append node metadata ----
    d3_nodes.append({
        "id": n,
        "label": label_text,
        "catalog": catalog_name,
        "schema": schema_name,
        "color": color_map.get(catalog_schema_map.get(n, "unknown"), "#999"),
        "up": u,
        "down": d,
        "degree": u + d,
        "columns": col_text,
        "total_columns": total_cols,
        "type": node_type,
        "jobs": jobs,
        "total_jobs": len(jobs)
    })

# ---- DEBUG: Show sample jobs for inspection ----
# sample = [n for n in d3_nodes if n["jobs"]][:3]
# print("=== SAMPLE JOB ENTRIES ===")
# print(json.dumps(sample, indent=2))

# ---- 8.5) Compute summary metrics --------------------------
d3_links = [{"source": s, "target": t} for (s, t) in G.edges()]

summary = {
    "total_nodes": len(d3_nodes),
    "total_edges": len(d3_links),
    "avg_up": (sum(x["up"] for x in d3_nodes) / max(1, len(d3_nodes))),
    "avg_down": (sum(x["down"] for x in d3_nodes) / max(1, len(d3_nodes))),
    "schemas": len(schemas)
}

# ---- 9) Helper HTML fragments ------------------------------
def make_top10_rows(items, use_down=False):
    rows = []
    for n, u, d in items:
        count = (d if use_down else u)
        rows.append(
            f"<tr><td style='padding:2px 6px;font-weight:bold;'>{count}</td>"
            f"<td style='padding:2px 6px;'>{n}</td></tr>"
        )
    return "\n".join(rows)

top10_html = ""
if ENABLE_TOP10:
    top10_html = f"""
<div id="top10-container" class="floating bottom-left">
  <button id="top10-toggle" class="btn">📊 Show Top 10</button>
  <div id="top10-panel" class="panel hidden">
    <h3>Top 10 Dependencies (Overall)</h3>
    <div class="cols">
      <div class="col">
        <h4>Most Upstream</h4>
        <table>{make_top10_rows(top_up, use_down=False)}</table>
      </div>
      <div class="col">
        <h4>Most Downstream</h4>
        <table>{make_top10_rows(top_down, use_down=True)}</table>
      </div>
    </div>
    <hr class="divider"/>
    <h3>Top 10 Dependencies (Views)</h3>
    <div class="cols">
      <div class="col">
        <h4>Most Upstream</h4>
        <table>{make_top10_rows(top_view_up, use_down=False)}</table>
      </div>
      <div class="col">
        <h4>Most Downstream</h4>
        <table>{make_top10_rows(top_view_down, use_down=True)}</table>
      </div>
    </div>
  </div>
</div>
"""

legend_html = ""
if ENABLE_LEGEND:
    legend_items = "\n".join(
        f"<div class='legend-item'><div class='legend-swatch' style='background:{color_map[s]}'></div>"
        f"{s} ({schema_counts.get(s,0)})</div>"
        for s in schemas
    )
    legend_html = f"""
<div id="legend-container" class="floating bottom-right">
  <button id="legend-toggle" class="btn">🎨 Show Legend</button>
  <div id="legend-panel" class="panel hidden">
    <h4>Catalog.Schema</h4>
    {legend_items}
  </div>
</div>
"""

summary_html = ""
if ENABLE_SUMMARY:
    summary_html = f"""
<div id="summary" class="floating top-left panel">
  <b>Lineage Summary</b><br/>
  Nodes: {summary['total_nodes']:,} &nbsp;|&nbsp; Edges: {summary['total_edges']:,}<br/>
  Avg Up: {summary['avg_up']:.1f} &nbsp;|&nbsp; Avg Down: {summary['avg_down']:.1f}<br/>
  Schemas: {summary['schemas']}
</div>
"""

# COMMAND ----------

# DBTITLE 1,Build HTML Header (Final)
# ---- 10) Build HTML header with JSON injection --------------
html_header = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>Databricks Lineage</title>
<style>
  :root {{
    --bg: #ffffff; --fg: #111111; --panel: rgba(255,255,255,0.96); --border:#cccccc;
    --link:#999999; --node-text:#222222;
  }}
  html.dark {{
    --bg: #0b0f14; --fg: #eaeef2; --panel: rgba(20,28,36,0.96); --border:#2a2f36;
    --link:#6b7a8a; --node-text:#eaeef2;
  }}
  body {{
    margin:0; overflow:hidden; background:var(--bg); color:var(--fg);
    font-family: Inter, system-ui, Arial, sans-serif;
  }}
  .floating {{ position: fixed; z-index: 9999; }}
  .top-left {{ top: 10px; left: 10px; }}
  .top-right {{ top: 10px; right: 10px; }}
  .bottom-left {{ bottom: 10px; left: 10px; }}
  .bottom-right {{ bottom: 10px; right: 10px; }}

  .panel {{
    background: var(--panel);
    border:1px solid var(--border);
    border-radius:10px;
    padding:10px 12px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    font-size: 13px;
  }}
  .btn {{
    background:#0c7bdc; color:#fff; border:none;
    border-radius:8px; padding:8px 12px;
    font-size:13px; cursor:pointer;
    box-shadow:0 2px 4px rgba(0,0,0,0.2);
  }}
  .btn:hover {{ filter: brightness(0.92); }}
  .hidden {{ display:none; }}

  .cols {{ display:flex; gap:20px; }}
  .col {{ flex:1; overflow:auto; max-height:260px; }}
  .divider {{ border:none; border-top:1px solid var(--border); margin:10px 0; }}

  .legend-item {{ display:flex; align-items:center; gap:8px; margin:3px 0; }}
  .legend-swatch {{ width:14px; height:14px; border-radius:3px; border:1px solid var(--border); }}

  /* Tooltip */
  .tooltip {{
    position: fixed;
    pointer-events: auto;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 10px;
    font-size: 12px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.2);
    display: none;
    max-width: 580px;
    white-space: normal;
    word-break: break-all;
    z-index: 10000;
  }}
  .tooltip .scrollbox {{
    pointer-events: auto;
  }}

  /* Graph */
  #graph {{ width:100vw; height:100vh; }}
  svg {{ width:100%; height:100%; }}
  line.link {{ stroke: var(--link); stroke-opacity:0.6; }}
  circle.node {{ stroke:#fff; stroke-width:1px; cursor:pointer; }}
  text.nlabel {{ font-size:10px; fill: var(--node-text); pointer-events:none; }}

  /* Search dropdown hover */
  #search-suggestions div:hover {{ background: rgba(12,123,220,0.25); }}

  /* Allow text selection globally */
  body, .panel, .tooltip, .floating, input, button, table, div, span {{
    user-select: text !important;
    -webkit-user-select: text !important;
    -moz-user-select: text !important;
    -ms-user-select: text !important;
  }}
</style>

<!-- 🔧 Inject workspace host + graph data -->
<script>
const DATABRICKS_HOST = "{workspace_url}";
const DATA = {{
  nodes: {json.dumps(d3_nodes)},
  links: {json.dumps(d3_links)}
}};
</script>
</head>
<body>

<div id="graph"></div>

{summary_html}
{top10_html}
{legend_html}

<!-- 🔍 Search + 🧭 Auto-Fit + 🌙 Dark/Light + 🧩 Spacing Combined Container -->
<div id="search-autofit-container" class="floating top-right panel"
     style="top:20px;right:20px;left:auto;display:flex;flex-direction:column;
            gap:14px;align-items:stretch;cursor:grab;
            padding:14px;background:var(--panel);border-radius:12px;
            box-shadow:0 3px 8px rgba(0,0,0,0.25);
            min-width:420px;max-width:520px;">

  <!-- Search Row -->
  <div style="display:flex;flex-direction:column;align-items:stretch;width:100%;
              gap:6px;position:relative;box-sizing:border-box;padding:0 4px;">
    <!-- Node Search -->
    <div style="position:relative;width:100%;box-sizing:border-box;">
      <input id="search-input" type="text" placeholder="🔍 Search nodes..."
        style="width:calc(100% - 8px);padding:8px 12px;margin:0 auto;
               border-radius:8px;border:1px solid var(--border);
               font-size:14px;background:var(--panel);
               color:var(--fg);display:block;box-sizing:border-box;">
      <div id="search-suggestions"
        style="position:absolute;top:38px;left:4px;width:calc(100% - 8px);
               max-height:260px;overflow-y:auto;background:var(--panel);
               border:1px solid var(--border);border-radius:8px;
               box-shadow:0 4px 8px rgba(0,0,0,0.25);
               display:none;z-index:10000;box-sizing:border-box;"></div>
    </div>

    <!-- Column Search -->
    <div style="position:relative;width:100%;">
      <input id="col-search-input" type="text" placeholder="🔍 Search columns..."
        style="width:100%;padding:8px 12px;border-radius:8px;
               border:1px solid var(--border);font-size:14px;
               background:var(--panel);color:var(--fg);
               display:block;box-sizing:border-box;">
    </div>

    <!-- Spacing Slider -->
    <div style="font-size:13px;width:100%;margin-top:2px;">
      <b>Spacing</b> (<span id="spacing-val">1.0</span>x)
      <input id="spacing-range" type="range" min="0.0" max="5" step="0.1" value="1.0"
             style="width:100%;margin-top:8px;height:8px;">
    </div>

    <!-- Buttons Row -->
    <div style="display:flex;gap:10px;width:100%;justify-content:space-between;">
      <button id="theme-btn" class="btn"
              style="flex:1;text-align:center;padding:10px 0;font-size:14px;">
        🌙 Dark Mode
      </button>
      <button id="unpin-btn" class="btn"
              style="flex:1;text-align:center;padding:10px 0;font-size:14px;">
        🧲 Unpin Nodes
      </button>
      <button id="autofit-btn" class="btn"
              style="flex:1;text-align:center;padding:10px 0;font-size:14px;">
        🧭 Auto-Fit
      </button>
    </div>
  </div>
</div>

<!-- Tooltip outside draggable container -->
<div id="tooltip" class="tooltip"></div>

<script src="https://unpkg.com/d3@7"></script>
"""

# COMMAND ----------

# DBTITLE 1,Build HTML Body
# ---- 11) JS body kept as raw string (no f-string parsing) ---
html_body = r"""
<style>
  #legend-panel {
    max-height: 70vh;
    overflow-y: auto;
    overflow-x: hidden;
    padding-right: 8px;
  }
  #legend-panel::-webkit-scrollbar {
    width: 8px;
  }
  #legend-panel::-webkit-scrollbar-thumb {
    background: rgba(200,200,200,0.35);
    border-radius: 4px;
  }

  /* --------------------------------------------------
     Tooltip Job Link — SAME BLUE in light + dark mode
     -------------------------------------------------- */
  #tooltip a {
    color: #1e90ff !important;     /* consistent blue */
    font-weight: 600;
    text-decoration: none;
  }

  #tooltip a:hover {
    color: #1c7ed6 !important;     /* slightly darker hover */
    text-decoration: underline !important;
  }

  html.dark #tooltip a {
    color: #1e90ff !important;     /* same blue in dark mode */
  }
</style>
<script>
// ------------------------------
// Bootstrap missing UI controls
// ------------------------------
(function ensureAutofitButton() {
  // If #autofit-btn already exists, do nothing
  if (document.getElementById('autofit-btn')) return;

  // Create container + button
  const c = document.createElement('div');
  c.id = 'autofit-container';
  c.className = 'floating';
  c.style.position = 'fixed';
  c.style.zIndex = '9999';

  // Try to anchor to the search container (to the right)
  const search = document.getElementById('search-container');
  if (search) {
    // Place to the right of search container
    const r = search.getBoundingClientRect();
    c.style.top = (search.style.top || '120px');
    // If search has explicit left, offset; else default near it
    const leftStr = search.style.left || '10px';
    const baseLeft = parseInt(leftStr, 10) || 10;
    c.style.left = (baseLeft + 280) + 'px'; // ~search width + padding
  } else {
    // Fallback: top-right
    c.style.top = '10px';
    c.style.right = '10px';
  }

  const btn = document.createElement('button');
  btn.id = 'autofit-btn';
  btn.className = 'btn';
  btn.textContent = '🧭 Auto-Fit';
  c.appendChild(btn);

  document.body.appendChild(c);
})();

(function ensureToggles() {
  // No-op: panels come from your header; this just ensures handlers attach later
})();

// ------------------------------
// Core state
// ------------------------------
const state = {dark:false,spacing:1.0};
const selectedNodes = new Set();

// Keep track of user-pinned nodes
const pinnedNodes = new Set();

// Theme toggle
const themeBtn = document.getElementById('theme-btn');
if (themeBtn) {
  themeBtn.addEventListener('click', () => {
    state.dark = !state.dark;
    document.documentElement.classList.toggle('dark', state.dark);
    themeBtn.textContent = state.dark ? '☀️ Light Mode' : '🌙 Dark Mode';
  });
}

// 🧲 Unpin all manually pinned nodes
const unpinBtn = document.getElementById('unpin-btn');
if (unpinBtn) {
  unpinBtn.addEventListener('click', (e) => {
    // ✅ Only respond to a *plain* click (no modifier keys)
    if (e.shiftKey || e.metaKey || e.altKey || e.ctrlKey) return;

    DATA.nodes.forEach(n => {
      n.fx = null;
      n.fy = null;
    });
    pinnedNodes.clear?.();
    sim.alpha(0.8).restart();

    // 💡 Optional visual feedback pulse
    unpinBtn.style.filter = 'brightness(1.4)';
    setTimeout(() => unpinBtn.style.filter = '', 300);
  });
}

// ✅ Restore panel-toggle handlers
function makeToggle(btnId, panelId) {
  const btn = document.getElementById(btnId);
  const panel = document.getElementById(panelId);
  if (!btn || !panel) return;
  btn.addEventListener('click', () => {
    panel.classList.toggle('hidden');
    btn.textContent = panel.classList.contains('hidden')
      ? btn.textContent.replace('Hide','Show')
      : btn.textContent.replace('Show','Hide');
  });
}
makeToggle('top10-toggle','top10-panel');
makeToggle('legend-toggle','legend-panel');

// Make panels draggable (only when clicking panel background)
function makeDraggable(sel) {
  const el = document.querySelector(sel);
  if (!el) return;
  el.style.position = 'fixed';

  el.addEventListener('mousedown', (e) => {
    // ✅ Only left-click
    if (e.button !== 0) return;

    // ✅ Don’t start dragging when clicking *inside* any child element
    // This ensures dragging only happens if you click directly on the panel background
    if (e.target !== el) return;

    const rect = el.getBoundingClientRect();
    const offX = e.clientX - rect.left;
    const offY = e.clientY - rect.top;

    el.style.cursor = 'grabbing';

    function onMove(ev) {
      el.style.left = (ev.clientX - offX) + 'px';
      el.style.top  = (ev.clientY - offY) + 'px';
      el.style.right = 'auto';
      el.style.bottom = 'auto';
    }

    function onUp() {
      el.style.cursor = 'grab';
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    }

    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);

    e.preventDefault();
  });
}

makeDraggable('#top10-container');
makeDraggable('#legend-container');
makeDraggable('#summary');
makeDraggable('#controls');
makeDraggable('#search-autofit-container'); // 🧭 draggable

// Spacing slider
const spacingInput = document.getElementById('spacing-range');
const spacingVal = document.getElementById('spacing-val');
if (spacingInput) {
  spacingInput.addEventListener('input', e => {
    state.spacing = +e.target.value;
    if (spacingVal) spacingVal.textContent = state.spacing.toFixed(1);
    sim.force('link').distance(linkDistance);
    sim.force('charge').strength(chargeStrength);
    sim.force('collide').radius(collideRadius);
    sim.alpha(0.7).restart();
  });
}

// Graph render
const width = window.innerWidth, height = window.innerHeight;
const svg = d3.select('#graph').append('svg')
  .attr('viewBox', [0,0,width,height])
  .call(d3.zoom().on('zoom', (e) => g.attr('transform', e.transform)));
const g = svg.append('g');
const defs = svg.append('defs');

// Arrowheads
const grad = defs.append('linearGradient')
  .attr('id', 'link-gradient')
  .attr('x1', '0%').attr('x2', '100%')
  .attr('y1', '0%').attr('y2', '0%');
grad.append('stop').attr('offset', '0%').attr('stop-color', '#999');
grad.append('stop').attr('offset', '100%').attr('stop-color', '#00ffff');
defs.append('marker')
  .attr('id', 'arrow')
  .attr('viewBox', '0 -5 10 10')
  .attr('refX', 26).attr('refY', 0)
  .attr('markerWidth', 6).attr('markerHeight', 6)
  .attr('orient', 'auto')
  .append('path')
  .attr('d', 'M0,-5L10,0L0,5')
  .attr('fill', 'url(#link-gradient)');

const link = g.append('g').selectAll('line')
  .data(DATA.links)
  .join('line')
  .attr('class','link')
  .attr('marker-end','url(#arrow)');

const node = g.append('g').selectAll('circle')
  .data(DATA.nodes, d=>d.id)
  .join('circle')
  .attr('class','node')
  .attr('r', d => 5 + Math.sqrt(d.degree+1))
  .attr('fill', d => d.color)
  .on('click', (e, d) => {
    if (e.ctrlKey || e.metaKey) {
      // Ctrl (Windows/Linux) or Cmd (Mac) + click → open Unity Catalog in new tab
      const cleanLabel = d.label.split('(')[0].trim();
      const ucUrl = `https://${DATABRICKS_HOST}/explore/data/${encodeURIComponent(d.catalog)}/${encodeURIComponent(d.schema)}/${encodeURIComponent(cleanLabel)}?activeTab=lineage`;
      window.open(ucUrl, '_blank', 'noopener,noreferrer');
    } else {
      // Normal click = select / spotlight
      toggleSpotlight(e, d.id);
    }
  })
  .on('mouseover', (e, d) => showTip(e, d))
  .on('mousemove', (e, d) => moveTip(e))
  .on('mouseout', (e, d) => {
    // ✅ Only hide tooltip if mouse truly leaves both node *and* tooltip
    const toEl = e.relatedTarget;
    if (!toEl || !toEl.closest('#tooltip')) hideTip();
  })
  .call(drag());

node.on('click', (e, d) => {
  if (e.altKey || e.metaKey && e.shiftKey) {
    // 🧭 Alt-click (or Option-click) unpins
    d.fx = null;
    d.fy = null;
    pinnedNodes.delete(d.id);
    return;
  }

  if (e.ctrlKey || e.metaKey) {
    // Ctrl/Cmd + click → open Unity Catalog
    const cleanLabel = d.label.split('(')[0].trim();
    const ucUrl = `https://${DATABRICKS_HOST}/explore/data/${encodeURIComponent(d.catalog)}/${encodeURIComponent(d.schema)}/${encodeURIComponent(cleanLabel)}?activeTab=lineage`;
    window.open(ucUrl, '_blank', 'noopener,noreferrer');
  } else {
    toggleSpotlight(e, d.id);
  }
});

const label = g.append('g').selectAll('text')
  .data(DATA.nodes, d=>d.id)
  .join('text')
  .attr('class','nlabel')
  .attr('dy', -10)
  .text(d => d.label);

// Background click clears selection only on single click, not double-click
let clickTimer = null;
svg.on('click', function(e) {
  if (e.target.tagName !== 'svg') return;
  // If a double-click is happening, cancel the clear
  if (clickTimer) {
    clearTimeout(clickTimer);
    clickTimer = null;
    return; // Skip single-click behavior (double-click will handle zoom)
  }
  clickTimer = setTimeout(() => {
    // ✅ Single-click background → clear selection and reset forces
    selectedNodes.clear();
    applySpotlight(true); // true = restore default forces

    // Reset spacing UI + forces
    state.spacing = 1.0;
    if (spacingInput) spacingInput.value = 1.0;
    if (spacingVal) spacingVal.textContent = '1.0';
    sim.force('link').distance(linkDistance);
    sim.force('charge').strength(chargeStrength);
    sim.force('collide').radius(collideRadius);
    sim.alpha(0.7).restart();

    clickTimer = null;
  }, 250); // 250ms delay allows double-click to cancel this
});

// Double-click background → zoom in (no unhighlight)
svg.on('dblclick', (event) => {
  if (event.target.tagName !== 'svg') return; // only zoom on background
  const [x, y] = d3.pointer(event);
  const zoomInFactor = 1.5;
  svg.transition().duration(400)
    .call(zoom.scaleBy, zoomInFactor, [x, y]);
});

// Forces with spacing factors
const linkDistance = l => 180*state.spacing + 20*Math.sqrt((l.source.degree||1)+(l.target.degree||1));
const chargeStrength = d => -600*state.spacing - 25*(d.degree||1);
const collideRadius = d => 22*state.spacing + Math.sqrt(d.degree+1);

let sim = d3.forceSimulation(DATA.nodes)
  .force('link', d3.forceLink(DATA.links).id(d=>d.id).distance(linkDistance).strength(0.3))
  .force('charge', d3.forceManyBody().strength(chargeStrength))
  .force('collide', d3.forceCollide().radius(collideRadius))
  .force('x', d3.forceX(width/2).strength(0.03))
  .force('y', d3.forceY(height/2).strength(0.03))
  .alphaDecay(0.03)
  .on('tick', ticked)
  .on('end', zoomToFit);

function ticked() {
  link
    .attr('x1', d=>d.source.x).attr('y1', d=>d.source.y)
    .attr('x2', d=>d.target.x).attr('y2', d=>d.target.y);
  node.attr('cx', d=>d.x).attr('cy', d=>d.y);
  label.attr('x', d=>d.x).attr('y', d=>d.y);
}

function drag() {
  let dragMoved = false;

  function dragstarted(event, d) {
    if (!event.active) sim.alphaTarget(0.3).restart();
    dragMoved = false;
  }

  function dragged(event, d) {
    // ✅ Ignore modifier-assisted drags
    if (event.shiftKey || event.metaKey || event.altKey || event.ctrlKey) return;

    // Detect meaningful movement (only pin if user actually moves node)
    if (Math.abs(event.dx) > 2 || Math.abs(event.dy) > 2) {
      dragMoved = true;
      d.fx = event.x;
      d.fy = event.y;
    }
  }

  function dragended(event, d) {
    if (!event.active) sim.alphaTarget(0);

    // ✅ Only pin if there was actual drag movement AND no modifiers
    if (dragMoved && !(event.shiftKey || event.metaKey || event.altKey || event.ctrlKey)) {
      pinnedNodes.add(d.id);
      d.fx = d.x;
      d.fy = d.y;
    } else {
      // No movement (click only) → let it float freely
      if (!pinnedNodes.has(d.id)) {
        d.fx = null;
        d.fy = null;
      }
    }
  }

  return d3.drag()
    .on('start', dragstarted)
    .on('drag', dragged)
    .on('end', dragended);
}


// ----------------------------------------------------
// 🔦 Multi-node selection with Ctrl/Cmd/Shift support
// ----------------------------------------------------
function toggleSpotlight(event, id) {
  const isMulti = event.ctrlKey || event.metaKey || event.shiftKey;
  if (!isMulti) selectedNodes.clear();
  if (selectedNodes.has(id)) selectedNodes.delete(id);
  else selectedNodes.add(id);
  applySpotlight();
}

function applySpotlight(restore = false) {
  if (selectedNodes.size === 0) {
    node.style('opacity', 1.0);
    label.style('opacity', 1.0);
    link.style('opacity', 0.7);

    // If requested, restore default global forces (original look)
    if (restore) {
      sim.force('link', d3.forceLink(DATA.links).id(d=>d.id).distance(linkDistance).strength(0.3));
      sim.force('charge', d3.forceManyBody().strength(chargeStrength));
      sim.force('collide', d3.forceCollide().radius(collideRadius));
      sim.force('x', d3.forceX(width/2).strength(0.03));
      sim.force('y', d3.forceY(height/2).strength(0.03));
      sim.alpha(0.5).restart();
    }
    return;
  }

  const connected = new Set([...selectedNodes]);
  DATA.links.forEach(l => {
    if (selectedNodes.has(l.source.id)) connected.add(l.target.id);
    if (selectedNodes.has(l.target.id)) connected.add(l.source.id);
  });

  node.style('opacity', d => connected.has(d.id) ? 1.0 : 0.08);
  label.style('opacity', d => connected.has(d.id) ? 1.0 : 0.05);
  link.style('opacity', l =>
    (selectedNodes.has(l.source.id) || selectedNodes.has(l.target.id)) ? 0.9 : 0.05
  );
}

// remember last searched column
let lastColumnQuery = null;  

// ===============================================
// FINAL FIXED TOOLTIP (Jobs + Highlighted Columns)
// ===============================================
const tip = document.getElementById('tooltip');

function showTip(e, d) {

  const colList = (d.columns || "").split(",").map(s => s.trim());
  const query = (lastColumnQuery || "").toLowerCase();

  // ---- Columns with highlight ----
  const highlightedCols = colList
    .map(name => {
      const match = name.toLowerCase() === query;
      return match
        ? `<span style="
            background: rgba(0,191,255,0.65);
            padding: 1px 3px;
            border-radius: 4px;
            font-weight: bold;
            box-shadow: 0 0 4px rgba(0,191,255,0.6);
          ">${name}</span>`
        : name;
    })
    .join(", ");

// ---- Jobs section ----

const jobsHTML =
  (d.jobs && d.jobs.length)
    ? d.jobs.map(j => {
        const name = j.job_name || j.jobName || j.name || "Unknown Job";
        const url  = j.job_url  || j.jobUrl  || j.url  || "#";

        return `
          <div style="margin-bottom:6px;">
            • <a href="${url}" target="_blank"
                 style="color:cyan;text-decoration:none;">
                 ${name}
              </a>
          </div>
        `;
      }).join("")
    : "<i>No upstream jobs found</i>";

  // ---- Final tooltip HTML ----

  tip.innerHTML = `
      <div style="font-size:12px;line-height:1.4;max-width:560px;">

        <b style="word-break:break-all;">${d.id}</b><br/>

        <div style="margin-top:2px;">
          Catalog.Schema:
          <code style="font-size:11px;white-space:normal;word-break:break-all;">
            ${d.schema}
          </code>
        </div>

        <div style="margin-top:4px;">
          Upstream: ${d.up} &nbsp;|&nbsp;
          Downstream: ${d.down} &nbsp;|&nbsp;
          Degree: ${d.degree} &nbsp;|&nbsp;
          Total Columns: ${d.total_columns} &nbsp;|&nbsp;
          Total Upstream Jobs: ${d.total_jobs}
        </div>

        <hr style="border:none;border-top:1px solid var(--border);margin:4px 0;">

        <b>Columns:</b><br/>
        <div style="max-height:180px;overflow-y:auto;
                    font-size:11px;white-space:normal;">
          ${highlightedCols}
        </div>

        <hr style="border:none;border-top:1px solid var(--border);margin:6px 0;">

        <b>Jobs:</b> (${d.total_jobs})
        <div style="max-height:160px;overflow-y:auto;
                    font-size:11px;white-space:normal;">
          ${jobsHTML}
        </div>

      </div>
  `;

  tip.style.display = "block";
  moveTip(e);
}

function moveTip(e) {
  const pad = 12;
  tip.style.left = (e.clientX + pad) + "px";
  tip.style.top  = (e.clientY + pad) + "px";
}

function hideTip() {
  tip.style.display = "none";
}

tip.addEventListener("mouseleave", hideTip);


// ----------------------------------------------------
// 🔍 Smart Search with Fuzzy Match + Highlight + Zoom
// ----------------------------------------------------
const searchBox = document.getElementById('search-input');
const suggestionsBox = document.getElementById('search-suggestions');
let selectedIndex = -1;

const nodeNames = DATA.nodes.map(n => n.id);
function fuzzyMatch(query, maxResults=500) {
  if (!query) return [];
  const q = query.toLowerCase();
  const scored = nodeNames
    .map(name => {
      const n = name.toLowerCase();
      const idx = n.indexOf(q);
      const score = (idx === -1) ? Infinity : idx + n.length * 0.001;
      return { name, score };
    })
    .filter(x => x.score !== Infinity)
    .sort((a,b) => a.score - b.score)
    .slice(0, maxResults);
  return scored.map(x => x.name);
}

function renderSuggestions(list) {
  suggestionsBox.innerHTML = '';
  if (!list.length) { suggestionsBox.style.display='none'; return; }
  list.forEach((name, i) => {
    const div = document.createElement('div');
    div.textContent = name;
    div.style.padding = '4px 8px';
    div.style.cursor = 'pointer';
    div.style.fontSize = '12px';
    if (i === selectedIndex) div.style.background = 'rgba(12,123,220,0.15)';
    div.addEventListener('click', () => selectNode(name));
    suggestionsBox.appendChild(div);
  });
  suggestionsBox.style.display = 'block';
}

searchBox.addEventListener('input', e => {
  const q = e.target.value.trim();
  if (!q) {
    suggestionsBox.style.display='none';
    node.style('opacity', 1);
    link.style('opacity', 0.7);
    label.style('opacity', 1);
    return;
  }
  const matches = fuzzyMatch(q);
  renderSuggestions(matches);
  const set = new Set(matches);
  node.style('opacity', d => set.has(d.id) ? 1 : 0.08);
  label.style('opacity', d => set.has(d.id) ? 1 : 0.05);
  link.style('opacity', 0.1);
});

// Allow ESC to close the suggestion box and reset state
searchBox.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    searchBox.value = '';                     // ✅ Clear the text box
    suggestionsBox.style.display = 'none';    // Hide dropdown
    selectedIndex = -1;
    node.style('opacity', 1);                 // Reset graph
    link.style('opacity', 0.7);
    label.style('opacity', 1);
    e.stopPropagation();
  }
});

searchBox.addEventListener('keydown', e => {
  const items = [...suggestionsBox.children];
  if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && items.length) {
    if (e.key === 'ArrowDown') selectedIndex = (selectedIndex + 1) % items.length;
    else selectedIndex = (selectedIndex - 1 + items.length) % items.length;
    renderSuggestions(items.map(x => x.textContent));
    e.preventDefault();
  } else if (e.key === 'Enter') {
    const items2 = [...suggestionsBox.children];
    const name = (selectedIndex >= 0 && items2[selectedIndex]) ? items2[selectedIndex].textContent : searchBox.value;
    if (name) selectNode(name);
    suggestionsBox.style.display = 'none';
    e.preventDefault();
  }
});

function selectNode(name) {
  const match = DATA.nodes.find(n => n.id.toLowerCase() === name.toLowerCase());
  if (!match) return;

  selectedNodes.clear();
  selectedNodes.add(match.id);
  applySpotlight();

  const scale = 1.8;
  const tx = width/2 - match.x*scale;
  const ty = height/2 - match.y*scale;
  svg.transition().duration(700)
     .call(d3.zoom().transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
  searchBox.value = match.id;
  suggestionsBox.style.display = 'none';
  selectedIndex = -1;
}

// =====================================================
// 🔍 Column Search (partial dropdown + exact-match highlight)
// =====================================================
const colSearchInput = document.getElementById('col-search-input');

// Create dropdown container
const colSuggestions = document.createElement('div');
colSuggestions.id = 'col-search-suggestions';
colSuggestions.style.cssText = `
  position:fixed;
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:8px;
  box-shadow:0 4px 8px rgba(0,0,0,0.25);
  max-height:260px;
  overflow-y:auto;
  display:none;
  z-index:999999;
`;
document.body.appendChild(colSuggestions);

colSearchInput.addEventListener('input', e => {
  const query = e.target.value.trim().toLowerCase();
  lastColumnQuery = query;
  colSuggestions.innerHTML = '';

  const rect = colSearchInput.getBoundingClientRect();
  colSuggestions.style.top = rect.bottom + 6 + 'px';
  colSuggestions.style.left = rect.left + 'px';
  colSuggestions.style.width = rect.width + 'px';

  if (!query) {
    colSuggestions.style.display = 'none';
    node.style('opacity', 1);
    label.style('opacity', 1);
    link.style('opacity', 0.7);
    return;
  }

  // ✅ Partial match suggestions for dropdown
  const allCols = Array.from(
    new Set(
      DATA.nodes.flatMap(n =>
        (n.columns || '')
          .split(',')
          .map(c => c.trim())
          .filter(c => c.toLowerCase().includes(query))
      )
    )
  ).sort();

  // Build dropdown list
  allCols.slice(0, 500).forEach(c => {
    const div = document.createElement('div');
    div.textContent = c;
    div.style.padding = '6px 10px';
    div.style.cursor = 'pointer';
    div.addEventListener('click', () => {
      colSearchInput.value = c;
      highlightByColumn(c.toLowerCase()); // exact match
      colSuggestions.style.display = 'none';
    });
    colSuggestions.appendChild(div);
  });

  colSuggestions.style.display = allCols.length ? 'block' : 'none';
});

// ✅ Enter = exact match search + Auto-Fit if matches exist
colSearchInput.addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    e.preventDefault();
    const query = e.target.value.trim().toLowerCase();
    if (!query) return;

    highlightByColumn(query);
    colSuggestions.style.display = 'none';

    if (selectedNodes.size > 0) {
      document.getElementById('autofit-btn')?.click();
    }
  } else if (e.key === 'Escape') {
    colSearchInput.value = '';
    colSuggestions.style.display = 'none';
    node.style('opacity', 1);
    label.style('opacity', 1);
    link.style('opacity', 0.7);
    e.stopPropagation();
  }
});

// Exact-match highlighter
function highlightByColumn(query) {
  lastColumnQuery = query;

  const matchingIds = new Set(
    DATA.nodes.filter(n =>
      (n.columns || '').split(',').map(c => c.trim().toLowerCase()).includes(query)
    ).map(n => n.id)
  );

  node.style('opacity', d => matchingIds.has(d.id) ? 1 : 0.08);
  label.style('opacity', d => matchingIds.has(d.id) ? 1 : 0.05);
  link.style('opacity', l =>
    (matchingIds.has(l.source.id) || matchingIds.has(l.target.id)) ? 0.7 : 0.05
  );

  selectedNodes.clear();
  matchingIds.forEach(id => selectedNodes.add(id));

  const autoFitBtn = document.getElementById('autofit-btn');
  if (autoFitBtn) autoFitBtn.disabled = matchingIds.size === 0;
}

// Escape key resets
colSearchInput.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    colSearchInput.value = '';
    colSuggestions.style.display = 'none';
    node.style('opacity', 1);
    label.style('opacity', 1);
    link.style('opacity', 0.7);
    e.stopPropagation();
  }
});

// ----------------------------------------------------
// 🧭 Hybrid Auto-Fit (Camera Fit + Dynamic Re-Layout)
// ----------------------------------------------------
document.getElementById('autofit-btn')?.addEventListener('click', ()=> {
  if (selectedNodes.size === 0) return;

  // Selected + 1-hop neighbors
  const connected = new Set([...selectedNodes]);
  DATA.links.forEach(l => {
    if (selectedNodes.has(l.source.id)) connected.add(l.target.id);
    if (selectedNodes.has(l.target.id)) connected.add(l.source.id);
  });
  const subset = DATA.nodes.filter(n => connected.has(n.id));
  if (!subset.length) return;

  // Camera fit on subset
  const minX = d3.min(subset, d=>d.x), maxX = d3.max(subset, d=>d.x);
  const minY = d3.min(subset, d=>d.y), maxY = d3.max(subset, d=>d.y);
  const w = (maxX - minX) || 1, h = (maxY - minY) || 1, pad = 150;
  const scale = 0.9 * Math.min(width / (w + pad), height / (h + pad));
  const tx = (width - scale * (minX + maxX)) / 2;
  const ty = (height - scale * (minY + maxY)) / 2;
  // Compute centroid and recenter precisely on it
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const centeredTx = width / 2 - cx * scale;
  const centeredTy = height / 2 - cy * scale;

  svg.transition().duration(800)
    .call(
      d3.zoom().transform,
      d3.zoomIdentity.translate(centeredTx, centeredTy).scale(scale * 1.3)
    );

  // 🔧 Dynamic spacing heuristic (subset size → spacing)
  const dynSpacing = Math.min(2.0, Math.max(0.6, subset.length / 40));
  state.spacing = dynSpacing;
  if (spacingInput) spacingInput.value = dynSpacing;
  if (spacingVal) spacingVal.textContent = dynSpacing.toFixed(1);

  // Refresh global forces using new spacing
  sim.force('link').distance(linkDistance);
  sim.force('charge').strength(chargeStrength);
  sim.force('collide').radius(collideRadius);

  // Tighten subset interactions
  const isTight = l => connected.has(l.source.id) && connected.has(l.target.id);
  const newLinkForce = d3.forceLink(DATA.links).id(d=>d.id)
    .distance(l => isTight(l) ? 60 : linkDistance(l))
    .strength(l => isTight(l) ? 1.0 : 0.3);

  sim.force('link', newLinkForce);
  sim.force('charge', d3.forceManyBody().strength(d => connected.has(d.id) ? -200 : chargeStrength(d)));
  sim.force('collide', d3.forceCollide().radius(d => connected.has(d.id) ? 12 : collideRadius(d)));
  sim.force('x', d3.forceX(width/2).strength(d => connected.has(d.id) ? 0.3 : 0.03));
  sim.force('y', d3.forceY(height/2).strength(d => connected.has(d.id) ? 0.3 : 0.03));
  // Keep pinned nodes locked during Auto-Fit
  sim.nodes().forEach(n => {
    if (pinnedNodes.has(n.id)) {
      n.fx = n.x;
      n.fy = n.y;
    }
  });
  sim.alpha(1.0).restart();
});

// ---------------------------------------------
// 🔍 Allow Enter key in Column Search to trigger Auto-Fit & hide dropdown
// ---------------------------------------------
document.getElementById('col-search-input')?.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault();
    const query = e.target.value.trim().toLowerCase();
    if (!query) return;

    // Highlight matching nodes
    highlightByColumn(query);

    // ✅ Hide the dropdown if visible
    const colSuggestions = document.getElementById('col-search-suggestions');
    if (colSuggestions) colSuggestions.style.display = 'none';

    // ✅ Trigger Auto-Fit if matches exist
    if (selectedNodes.size > 0) {
      document.getElementById('autofit-btn')?.click();
    }
  }
});

// First-time zoom-to-fit after layout cools
let hasZoomedOnce = false;
function zoomToFit() {
  if (hasZoomedOnce) return;
  hasZoomedOnce = true;
  const nodes = DATA.nodes;
  if (!nodes.length) return;
  const minX = d3.min(nodes, d => d.x), maxX = d3.max(nodes, d => d.x);
  const minY = d3.min(nodes, d => d.y), maxY = d3.max(nodes, d => d.y);
  const w = (maxX - minX) || 1, h = (maxY - minY) || 1;
  const pad = 150;
  const scale = 0.9 * Math.min(width / (w + pad), height / (h + pad));
  const tx = (width - scale * (minX + maxX)) / 2;
  const ty = (height - scale * (minY + maxY)) / 2;
  svg.transition().duration(800)
    .call(d3.zoom().transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}

// Resize handling
window.addEventListener('resize', () => {
  const w = window.innerWidth, h = window.innerHeight;
  svg.attr('viewBox', [0,0,w,h]);
  sim.force('x', d3.forceX(w/2).strength(0.03));
  sim.force('y', d3.forceY(h/2).strength(0.03));
  sim.alpha(0.3).restart();
});
</script>
</body>
</html>
"""

# ---- 12) Write to file (safe) -------------------------------
html = html_header + html_body
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
if os.path.exists(OUTPUT_PATH):
    shutil.copy(OUTPUT_PATH, OUTPUT_PATH + ".bak")

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(html)

print(f"✅ Exported lineage graph (Unified Pro + Hybrid 🧭 Auto-Fit) → {OUTPUT_PATH}")

# COMMAND ----------

