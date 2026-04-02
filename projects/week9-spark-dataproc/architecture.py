# architecture.py
# Generates the Week 9 architecture diagram as a PNG
# Run once locally — output saved to architecture.png

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(1, 1, figsize=(16, 9))
ax.set_xlim(0, 16)
ax.set_ylim(0, 9)
ax.axis('off')
fig.patch.set_facecolor('#0f1117')
ax.set_facecolor('#0f1117')

def box(x, y, w, h, color, label, sublabel=""):
    rect = FancyBboxPatch((x, y), w, h,
        boxstyle="round,pad=0.1",
        facecolor=color, edgecolor='white', linewidth=1.5, alpha=0.9)
    ax.add_patch(rect)
    ax.text(x + w/2, y + h/2 + (0.15 if sublabel else 0), label,
        ha='center', va='center', fontsize=9, fontweight='bold', color='white')
    if sublabel:
        ax.text(x + w/2, y + h/2 - 0.22, sublabel,
            ha='center', va='center', fontsize=7, color='#cccccc')

def arrow(x1, y1, x2, y2):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle='->', color='#4fc3f7', lw=2))

def label(x, y, text, size=8, color='#aaaaaa'):
    ax.text(x, y, text, ha='center', va='center', fontsize=size, color=color)

# Title
ax.text(8, 8.5, 'Week 9 — Spark & Batch Processing Architecture',
    ha='center', va='center', fontsize=14, fontweight='bold', color='white')

# Row 1 — Data Sources
box(0.5, 6.5, 2.2, 1.0, '#1565c0', 'Raw CSV Data',     'orders_large.csv')
box(3.0, 6.5, 2.2, 1.0, '#1565c0', 'Customers Table',  'In-memory lookup')
box(5.5, 6.5, 2.2, 1.0, '#1565c0', 'GCS Bucket',       'gs://project-spark-week9')
label(7.5, 7.0, 'DATA SOURCES', 9, '#4fc3f7')

# Row 2 — Local Spark
box(0.5, 4.8, 2.2, 1.0, '#2e7d32', 'Day 1–2',   'Lazy eval · DataFrame API')
box(3.0, 4.8, 2.2, 1.0, '#2e7d32', 'Day 3 ETL', 'Modular pipeline · Tests')
box(5.5, 4.8, 2.2, 1.0, '#2e7d32', 'Day 5 Perf','Broadcast · Cache · AQE')

label(3.85, 6.3, '▼  local[*]', 8, '#81c784')
label(3.85, 4.5, 'LOCAL PYSPARK (Windows)', 9, '#4fc3f7')

# Row 3 — Dataproc
box(9.0, 6.0, 3.0, 1.2, '#6a1b9a', 'Dataproc Serverless', 'asia-south1 · Spark 3.5.1')
box(9.0, 4.5, 3.0, 1.2, '#4527a0', 'Spark Job',           'day4_dataproc_bigquery.py')

label(10.5, 5.85, '▼', 10, '#ce93d8')
label(10.5, 4.2,  '▼', 10, '#ce93d8')
label(10.5, 3.8, 'GOOGLE CLOUD PLATFORM', 9, '#4fc3f7')

# Row 4 — BigQuery
box(9.0, 3.0, 3.0, 1.2, '#e65100', 'BigQuery',           'week9_spark dataset')

# BQ Tables
box(8.2,  1.3, 1.8, 1.2, '#bf360c', 'orders_enriched',      '~300 rows')
box(10.2, 1.3, 1.8, 1.2, '#bf360c', 'report_city_category', '60 rows')
box(12.2, 1.3, 1.8, 1.2, '#bf360c', 'report_monthly_trend', '7 rows')
box(14.2, 1.3, 1.8, 1.2, '#bf360c', 'report_value_tiers',   '3 rows')

label(11.5, 2.85, '▼  writes 4 tables', 8, '#ff8a65')

# Optimizations callout
box(13.0, 5.5, 2.8, 2.8, '#1a237e', 'Optimizations', '')
items = [
    '✓ Broadcast joins',
    '✓ Cache (9.4x faster)',
    '✓ AQE enabled',
    '✓ Partition pruning',
    '✓ Skew detection',
    '✓ coalesce() on write',
]
for i, item in enumerate(items):
    ax.text(13.1, 8.0 - i*0.38, item, fontsize=7.5, color='#90caf9', va='center')

# Tests callout
box(13.0, 3.0, 2.8, 2.2, '#1b5e20', 'Testing', '')
tests = [
    '✓ 9 pytest unit tests',
    '✓ conftest.py fixture',
    '✓ clean_orders()',
    '✓ transform_orders()',
    '✓ build_tier_report()',
]
for i, t in enumerate(tests):
    ax.text(13.1, 4.9 - i*0.38, t, fontsize=7.5, color='#a5d6a7', va='center')

# Arrows — data flow
arrow(1.6, 6.5, 1.6, 5.8)
arrow(4.1, 6.5, 4.1, 5.8)
arrow(6.6, 6.5, 10.5, 7.2)
arrow(10.5, 6.0, 10.5, 5.7)
arrow(10.5, 4.5, 10.5, 4.2)

# Arrow local → GCP
ax.annotate('', xy=(9.0, 6.6), xytext=(7.7, 6.6),
    arrowprops=dict(arrowstyle='->', color='#4fc3f7', lw=1.5, linestyle='dashed'))
label(8.35, 6.85, 'gsutil cp', 7, '#4fc3f7')

plt.tight_layout()
plt.savefig('architecture.png', dpi=150, bbox_inches='tight',
            facecolor='#0f1117', edgecolor='none')
print("architecture.png saved!")