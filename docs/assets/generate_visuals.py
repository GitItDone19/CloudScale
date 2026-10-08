"""Regenerate the README's dependency-free SVG illustrations."""

from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BG, PANEL, LINE, TEXT, MUTED = "#0b1220", "#142237", "#30445f", "#f1f5f9", "#a9bbcf"
TEAL, BLUE, GOLD, PURPLE = "#5eead4", "#7db5ff", "#fbbf77", "#c4a5ff"


def text(x, y, value, size=18, color=TEXT, weight=400):
    return f'<text x="{x}" y="{y}" fill="{color}" font-size="{size}" font-weight="{weight}">{escape(value)}</text>'


def box(x, y, w, h, title, lines, accent=TEAL, tag=None):
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{PANEL}" stroke="{LINE}"/>'
    s += f'<rect x="{x+20}" y="{y+22}" width="30" height="4" rx="2" fill="{accent}"/>'
    s += text(x + 20, y + 59, title, 23, TEXT, 650)
    for i, line in enumerate(lines):
        s += text(x + 20, y + 92 + i * 27, line, 17, MUTED)
    if tag:
        s += text(x + 20, y + h - 19, tag, 14, accent, 650)
    return s


def arrow(x1, y1, x2, y2, color=TEAL, dashed=False):
    dash = ' stroke-dasharray="7 6"' if dashed else ""
    return f'<path d="M{x1},{y1} L{x2},{y2}" fill="none" stroke="{color}" stroke-width="2"{dash} marker-end="url(#arrow)"/>'


def save(name, title, desc, height, body):
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="{height}" viewBox="0 0 1200 {height}" role="img" aria-labelledby="title desc">
<title id="title">{escape(title)}</title><desc id="desc">{escape(desc)}</desc>
<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8" fill="{TEAL}"/></marker></defs>
<rect width="1200" height="{height}" rx="22" fill="{BG}"/>
<g font-family="Segoe UI, Arial, sans-serif">{body}</g></svg>"""
    (ROOT / name).write_text(svg, encoding="utf-8")


s = text(48, 53, "CLOUDSCALE / DATA ENGINEERING LAB", 15, TEAL, 650)
s += text(48, 112, "From messy logistics data", 44, TEXT, 700)
s += text(48, 165, "to trusted analytics.", 44, TEXT, 700)
s += text(
    48,
    209,
    "A learning project in ingestion, distributed processing and warehouse design.",
    20,
    MUTED,
)
for x, title, lines, accent, tag in [
    (
        48,
        "Collect",
        ["Orders, warehouse exports", "and delivery scanner events"],
        TEAL,
        "LOCAL BRONZE IMPLEMENTED",
    ),
    (
        424,
        "Refine",
        ["Validate, deduplicate", "and quarantine bad records"],
        BLUE,
        "PYSPARK / PLANNED",
    ),
    (
        800,
        "Understand",
        ["Delivery SLAs, warehouse", "efficiency and route margins"],
        PURPLE,
        "DBT + BIGQUERY / PLANNED",
    ),
]:
    s += box(x, 250, 352, 172, title, lines, accent, tag)
s += (
    text(48, 465, "PRESERVE THE SOURCE", 14, TEAL, 650)
    + text(424, 465, "MAKE EVERY RERUN SAFE", 14, BLUE, 650)
    + text(800, 465, "BUILD EXPLAINABLE PIPELINES", 14, PURPLE, 650)
)
save(
    "cloudscale-banner.svg",
    "CloudScale: from messy logistics data to trusted analytics",
    "Collect is implemented locally; PySpark refinement and warehouse analytics are planned.",
    495,
    s,
)

s = text(40, 48, "THE PLATFORM / TARGET ARCHITECTURE", 24, TEXT, 650)
s += text(
    40,
    80,
    "Solid arrows show the data path. Labels distinguish implemented and planned components.",
    17,
    MUTED,
)
items = [
    (
        40,
        "Sources",
        ["Generated CSV + JSON", "Orders / merchants", "Warehouse / carriers"],
        TEAL,
        "IMPLEMENTED LOCALLY",
    ),
    (
        330,
        "Bronze",
        ["Original bytes preserved", "Date-partitioned files", "Checksums + metadata"],
        GOLD,
        "IMPLEMENTED LOCALLY",
    ),
    (
        620,
        "Silver",
        ["PySpark validation", "Event deduplication", "Clean Snappy Parquet"],
        BLUE,
        "PLANNED",
    ),
    (
        910,
        "Gold + marts",
        ["BigQuery + dbt models", "Facts and dimensions", "SLA / margin analytics"],
        PURPLE,
        "PLANNED",
    ),
]
for x, title, lines, accent, tag in items:
    s += box(x, 125, 250, 225, title, lines, accent, tag)
for x in (290, 580, 870):
    s += arrow(x, 235, x + 40, 235)
s += box(
    620,
    410,
    250,
    150,
    "Dead-letter queue",
    ["Rejected rows + reasons"],
    GOLD,
    "PLANNED / INVESTIGATE",
)
s += arrow(745, 350, 745, 410)
s += box(
    910,
    410,
    250,
    150,
    "BI dashboard",
    ["Operational decisions"],
    PURPLE,
    "PLANNED / LOOKER STUDIO",
)
s += arrow(1035, 350, 1035, 410)
s += box(
    40,
    410,
    540,
    150,
    "Airflow orchestration",
    ["Schedule tasks, retry failures, record execution state"],
    TEAL,
    "DOCKER SETUP EXISTS / FULL PIPELINE PLANNED",
)
s += text(
    40,
    610,
    "Local today: filesystem storage. Cloud target: GCS landing + Silver, BigQuery warehouse.",
    18,
    MUTED,
)
save(
    "platform-architecture.svg",
    "CloudScale platform architecture",
    "Generated sources flow into implemented local Bronze. Planned Silver processing routes rejected rows to a dead-letter queue; BigQuery and dbt supply analytics and dashboards. Full Airflow orchestration is planned.",
    645,
    s,
)

s = text(40, 48, "BRONZE / WHAT HAPPENS WHEN YOU INGEST A FILE?", 24, TEXT, 650)
s += text(
    40,
    80,
    "Idempotency means repeating the same input produces the same stored result.",
    18,
    MUTED,
)
s += box(
    40,
    120,
    310,
    170,
    "1. Read the source",
    ["Generated files in data/raw", "Calculate a SHA-256 fingerprint"],
    TEAL,
)
s += box(
    440,
    120,
    310,
    170,
    "2. Check destination",
    ["Select source + date partition", "Compare actual file contents"],
    BLUE,
)
s += box(
    840,
    120,
    320,
    170,
    "3. Decide safely",
    ["New, identical, or conflicting?", "Choose the matching action"],
    PURPLE,
)
s += arrow(350, 200, 440, 200) + arrow(750, 200, 840, 200)
for x, title, lines, accent in [
    (
        40,
        "New file: land",
        [
            "Copy to a temporary file",
            "Verify and publish complete data",
            "Write the metadata sidecar",
        ],
        TEAL,
    ),
    (
        440,
        "Identical file: skip",
        [
            "Keep existing data and metadata",
            "Preserve the ingestion timestamp",
            "Repair missing / invalid metadata",
        ],
        BLUE,
    ),
    (
        840,
        "Changed file: reject",
        [
            "Protect the existing partition",
            "Use a new date for a new snapshot",
            "Explicit --overwrite can replace",
        ],
        GOLD,
    ),
]:
    s += box(x, 355, 320, 205, title, lines, accent)
s += text(
    40,
    610,
    "Destination: data/bronze/{source}/dt=YYYY-MM-DD/    |    Original dirty data stays intact.",
    19,
    MUTED,
)
save(
    "bronze-ingestion.svg",
    "Bronze ingestion and safe reruns",
    "New files are staged and published. Identical files are skipped or have metadata repaired. Changed input is rejected unless overwrite is explicitly requested.",
    645,
    s,
)

s = text(40, 48, "LOCAL DEVELOPMENT / DOCKER COMPOSE DESIGN", 24, TEXT, 650)
s += text(
    40,
    80,
    "Configuration exists. Container startup and healthcheck DAG execution still need runtime validation.",
    17,
    MUTED,
)
s += box(
    40,
    130,
    320,
    170,
    "Your project files",
    ["DAGs, ingestion code and data", "Shared through folder mounts"],
    TEAL,
)
s += box(
    440,
    130,
    320,
    170,
    "Airflow webserver",
    ["Workflow interface", "localhost:8080"],
    BLUE,
)
s += box(
    840,
    130,
    320,
    170,
    "Airflow scheduler",
    ["Schedules and executes tasks", "LocalExecutor configuration"],
    BLUE,
)
s += box(
    440,
    365,
    320,
    170,
    "PostgreSQL",
    ["Airflow task state and history", "Metadata database"],
    GOLD,
)
s += arrow(600, 300, 600, 365)
s += (
    arrow(1000, 300, 1000, 337)
    + arrow(1000, 337, 790, 337)
    + arrow(790, 337, 790, 440)
    + arrow(790, 440, 760, 440)
)
s += box(
    40,
    365,
    320,
    170,
    "Spark master + worker",
    ["Distributed processing services", "Cleansing jobs arrive in phase 5"],
    PURPLE,
)
s += arrow(200, 300, 200, 365, dashed=True)
s += arrow(360, 210, 440, 210, dashed=True)
s += text(
    40,
    590,
    "Airflow init prepares the metadata database and development user before Airflow services start.",
    18,
    MUTED,
)
s += text(
    40,
    621,
    "Solid arrows: metadata access. Dashed arrows: shared project folders. No full ingestion DAG yet.",
    17,
    MUTED,
)
save(
    "local-development.svg",
    "Local Docker development design",
    "Project folders are mounted into Airflow and Spark. Airflow webserver and scheduler use PostgreSQL for metadata. Startup is not yet runtime verified.",
    655,
    s,
)
