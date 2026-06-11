"""
CS2 Market Data Warehouse — Analysis Dashboard
================================================
Reads live data from your Postgres DW and renders 9 charts,
each saved as a separate PNG file.

Usage
-----
    # defaults (localhost / CSGO_MARKET2 / postgres / admin)
    python cs2_analysis_postgres.py

    # override via environment variables
    DW_HOST=myserver DW_PASSWORD=secret python cs2_analysis_postgres.py

Dependencies
------------
    pip install psycopg2-binary pandas matplotlib numpy
"""

import os
import sys
import textwrap
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit("psycopg2 not found. Run:  pip install psycopg2-binary")

# ── CONNECTION CONFIG ─────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DW_HOST",     "localhost"),
    "port":     int(os.getenv("DW_PORT", "5432")),
    "dbname":   os.getenv("DW_NAME",     "CSGO_MARKET2"),
    "user":     os.getenv("DW_USER",     "postgres"),
    "password": os.getenv("DW_PASSWORD", "admin"),
}

OUTPUT_DIR = "cs2_charts"

# ── PALETTE ───────────────────────────────────────────────────────────────────
BG     = "#0d0f14"
PANEL  = "#15181f"
BORDER = "#22262e"
TEXT   = "#e8eaf0"
SUB    = "#7a8090"
GOLD   = "#f0b429"
BLUE   = "#4fc3f7"
RED    = "#ef5350"
GREEN  = "#66bb6a"
PURPLE = "#ce93d8"
ORANGE = "#ffa726"

RARITY_COLORS = {
    "Consumer":    "#aab4be",
    "Industrial":  "#5e98d9",
    "Mil-spec":          "#4b69ff",
    "Restricted":        "#8847ff",
    "Classified":        "#d32ce6",
    "Covert":            "#eb4b4b",
    "Contraband":        "#e4ae39",
}
WEAR_COLORS = {
    "Factory New":    "#66bb6a",
    "Minimal Wear":   "#aed581",
    "Field-Tested":   "#ffd54f",
    "Well-Worn":      "#ffa726",
    "Battle-Scarred": "#ef5350",
}

WEAR_ORDER   = ["Factory New", "Minimal Wear", "Field-Tested", "Well-Worn", "Battle-Scarred"]
RARITY_ORDER = ["Consumer", "Industrial", "Mil-spec",
                "Restricted", "Classified", "Covert", "Contraband"]

# ── HELPERS ───────────────────────────────────────────────────────────────────
def connect():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"  ✓ Connected to {DB_CONFIG['dbname']} on {DB_CONFIG['host']}")
        return conn
    except psycopg2.OperationalError as e:
        sys.exit(f"\n✗ Cannot connect to Postgres:\n  {e}\n"
                 "  Set DW_HOST / DW_PORT / DW_NAME / DW_USER / DW_PASSWORD env vars.")

def q(conn, sql, label="query"):
    print(f"  → {label} …", end=" ", flush=True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_sql_query(textwrap.dedent(sql), conn)
    print(f"{len(df)} rows")
    return df

def to_dt(series):
    """Parse a datetime series from Postgres, handling tz-aware values safely."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)

def style_ax(ax, title, xlabel="", ylabel=""):
    ax.set_facecolor(PANEL)
    for sp in ax.spines.values():
        sp.set_edgecolor(BORDER)
    ax.tick_params(colors=SUB, labelsize=8.5)
    ax.xaxis.label.set_color(SUB)
    ax.yaxis.label.set_color(SUB)
    ax.set_title(title, color=TEXT, fontsize=13, fontweight="bold", pad=12)
    if xlabel: ax.set_xlabel(xlabel, fontsize=9)
    if ylabel: ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(color=BORDER, linewidth=0.6, linestyle="--", alpha=0.7)

def save(fig, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  ✓ Saved {path}")

usd  = FuncFormatter(lambda x, _: f"${x:,.0f}")
mill = FuncFormatter(lambda x, _: f"{x/1e6:.1f}M")
mult = FuncFormatter(lambda x, _: f"{x:.1f}×")

# ── QUERIES ───────────────────────────────────────────────────────────────────
SQL = {}

SQL["rarity_trend"] = """
    SELECT
        DATE_TRUNC('month', f.date_id)   AS month,
        ds.rarity,
        AVG(f.price)                     AS avg_price
    FROM  fact_marketprice f
    JOIN  dim_skin          ds ON ds.skin_id = f.skin_id
    WHERE f.price IS NOT NULL
      AND ds.rarity IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

SQL["monthly_volume"] = """
    SELECT
        DATE_TRUNC('month', f.date_id) AS month,
        SUM(f.volume)                  AS total_volume
    FROM  fact_marketprice f
    WHERE f.volume IS NOT NULL
    GROUP BY 1
    ORDER BY 1
"""

SQL["rarity_share"] = """
    SELECT
        ds.rarity,
        SUM(f.volume)           AS total_volume,
        SUM(f.price * f.volume) AS total_revenue
    FROM  fact_marketprice f
    JOIN  dim_skin          ds ON ds.skin_id = f.skin_id
    WHERE f.price IS NOT NULL
      AND f.volume IS NOT NULL
      AND ds.rarity IS NOT NULL
    GROUP BY 1
"""

SQL["price_range_dist"] = """
    SELECT
        dpr.price_range,
        COUNT(*) AS listing_count
    FROM  fact_marketprice f
    JOIN  dim_price_range   dpr ON dpr.price_range_id = f.price_range_id
    GROUP BY 1
    ORDER BY 1
"""

SQL["wear_price"] = """
    SELECT
        dwr.wear_range           AS wear,
        AVG(f.price)             AS avg_price,
        COUNT(*)                 AS listings
    FROM  fact_marketprice f
    JOIN  dim_wear_range    dwr ON dwr.wear_range_id = f.wear_range_id
    WHERE f.price IS NOT NULL
    GROUP BY 1
    ORDER BY avg_price DESC
"""

SQL["weapon_bubble"] = """
    SELECT
        dw.weapon_type,
        SUM(f.volume)   AS total_volume,
        AVG(f.price)    AS avg_price
    FROM  fact_marketprice f
    JOIN  dim_weapon        dw ON dw.weapon_id = f.weapon_id
    WHERE f.price IS NOT NULL
      AND f.volume IS NOT NULL
    GROUP BY 1
"""

SQL["top10_weapons"] = """
    SELECT
        dw.weapon_name,
        AVG(f.price) AS avg_price
    FROM fact_marketprice f
    JOIN dim_weapon dw
        ON dw.weapon_id = f.weapon_id
    WHERE f.price IS NOT NULL
        AND dw.weapon_name NOT LIKE '%StatTrak%'
        AND dw.weapon_name NOT LIKE '%Gloves%'
        AND dw.weapon_name NOT LIKE '%Wraps%'
        AND dw.weapon_name NOT LIKE '%Souvenir%'
    GROUP BY dw.weapon_name
    ORDER BY avg_price DESC
    LIMIT 10;
"""

SQL["top10_weapons_no_gloves_knives"] = """
    SELECT
        dw.weapon_name,
        AVG(f.price) AS avg_price
    FROM fact_marketprice f
    JOIN dim_weapon dw
        ON dw.weapon_id = f.weapon_id
    WHERE f.price IS NOT NULL
        AND dw.weapon_name NOT LIKE '%StatTrak%'
        AND dw.weapon_name NOT LIKE '%Gloves%'
        AND dw.weapon_name NOT LIKE '%Wraps%'
        AND dw.weapon_name NOT LIKE '%Souvenir%'
        AND dw.weapon_type != 'Knife'
    GROUP BY dw.weapon_name
    ORDER BY avg_price DESC
    LIMIT 10;
"""

SQL["heatmap"] = """
    SELECT
        ds.rarity,
        dwr.wear_range AS wear,
        AVG(f.price)   AS avg_price
    FROM  fact_marketprice f
    JOIN  dim_skin          ds  ON ds.skin_id       = f.skin_id
    JOIN  dim_wear_range    dwr ON dwr.wear_range_id = f.wear_range_id
    WHERE f.price IS NOT NULL
      AND ds.rarity IS NOT NULL
    GROUP BY 1, 2
"""

SQL["container_age"] = """
    SELECT
        dc.container_name,
        dc.container_type,
        dc.release_date,
        AVG(f.price)  AS avg_price,
        MIN(f.price)  AS min_price
    FROM  fact_marketprice f
    JOIN  dim_container     dc ON dc.container_id = f.container_id
    WHERE f.price IS NOT NULL
      AND dc.release_date IS NOT NULL
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 10
"""

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\n── CS2 Market DW Analysis ──────────────────────────────────")
    print(f"   Output directory: {os.path.abspath(OUTPUT_DIR)}\n")

    conn = connect()

    print("\n[1/2] Fetching data …")
    rarity_trend   = q(conn, SQL["rarity_trend"],      "rarity trend")
    monthly_volume = q(conn, SQL["monthly_volume"],    "monthly volume")
    rarity_share   = q(conn, SQL["rarity_share"],      "rarity share")
    price_range_df = q(conn, SQL["price_range_dist"],  "price range dist")
    wear_price     = q(conn, SQL["wear_price"],        "wear price")
    weapon_bubble  = q(conn, SQL["weapon_bubble"],     "weapon bubble")
    top10          = q(conn, SQL["top10_weapons"],     "top 10 weapons")
    top10_filtered = q(conn, SQL["top10_weapons_no_gloves_knives"], "top 10 weapons (no gloves/knives)")
    heatmap_df     = q(conn, SQL["heatmap"],           "rarity×wear heatmap")

    has_container = False
    container_df  = pd.DataFrame()
    try:
        container_df  = q(conn, SQL["container_age"], "container age")
        has_container = len(container_df) > 0
    except Exception:
        print("  ⚠ No container_id on fact_marketprice — chart ⑧ will be skipped")
        conn.rollback()

    conn.close()

    # ── shared derivations ────────────────────────────────────────────────────
    rs = rarity_share.copy()
    if not rs.empty:
        tot_vol = rs["total_volume"].sum()
        tot_rev = rs["total_revenue"].sum()
        rs["vol_pct"] = rs["total_volume"] / tot_vol * 100
        rs["rev_pct"] = rs["total_revenue"] / tot_rev * 100
        rs = rs.sort_values("rev_pct", ascending=False)

    # heatmap pivot
    pivot = pd.DataFrame()
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot(index="rarity", columns="wear", values="avg_price")
        r_rows = [r for r in RARITY_ORDER if r in pivot.index]
        w_cols = [w for w in WEAR_ORDER   if w in pivot.columns]
        pivot  = pivot.loc[r_rows, w_cols]

    # container prep
    if has_container:
        now = pd.Timestamp.utcnow().tz_localize(None)
        # ── FIX: use utc=True then strip tz ──────────────────────────────────
        container_df["release_date"] = to_dt(container_df["release_date"])
        container_df["age_days"] = (now - container_df["release_date"]).dt.days
        global_median = wear_price["avg_price"].median() if not wear_price.empty else 1.0
        container_df["multiplier"] = container_df["avg_price"] / max(global_median, 0.01)

    print("\n[2/2] Building & saving charts …\n")

    # ① price trend by rarity ─────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 6), facecolor=BG)
    style_ax(ax, "Average Skin Price Over Time — Grouped by Rarity", ylabel="Avg Price (USD)")
    if not rarity_trend.empty:
        # ── FIX: use utc=True then strip tz ──────────────────────────────────
        rarity_trend["month"] = to_dt(rarity_trend["month"])
        for rarity, grp in rarity_trend.groupby("rarity"):
            c  = RARITY_COLORS.get(rarity, SUB)
            lw = 2.2 if rarity in ("Covert", "Classified") else 1.3
            al = 1.0 if rarity in ("Covert", "Classified") else 0.7
            ax.plot(grp["month"], grp["avg_price"], color=c, lw=lw, alpha=al, label=rarity)
        ax.yaxis.set_major_formatter(usd)
        ax.tick_params(axis="x", labelsize=8, colors=SUB)
        ax.legend(fontsize=9, loc="upper left",
                  facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT, ncol=2)
    save(fig, "01_rarity_price_trend.png")

  # ① price trend by rarity without contraband ─────────────────────────────────────────────────
  
    fig, ax = plt.subplots(figsize=(14, 6), facecolor=BG)
    style_ax(ax, "Average Skin Price Over Time — Grouped by Rarity", ylabel="Avg Price (USD)")
    if not rarity_trend.empty:
        # ── FIX: use utc=True then strip tz ──────────────────────────────────
        rarity_trend["month"] = to_dt(rarity_trend["month"])
        for rarity, grp in rarity_trend[rarity_trend["rarity"] != "Contraband"].groupby("rarity"):
            c  = RARITY_COLORS.get(rarity, SUB)
            lw = 2.2 if rarity in ("Covert", "Classified") else 1.3
            al = 1.0 if rarity in ("Covert", "Classified") else 0.7
            ax.plot(grp["month"], grp["avg_price"], color=c, lw=lw, alpha=al, label=rarity)
        ax.yaxis.set_major_formatter(usd)
        ax.tick_params(axis="x", labelsize=8, colors=SUB)
        ax.legend(fontsize=9, loc="upper left",
                  facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT, ncol=2)
    save(fig, "01_rarity_price_trend_no_contraband.png")

    # ② monthly volume ────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
    style_ax(ax, "Monthly Trade Volume", ylabel="Units Sold")
    if not monthly_volume.empty:
        # ── FIX: use utc=True then strip tz ──────────────────────────────────
        monthly_volume["month"] = to_dt(monthly_volume["month"])
        mv = monthly_volume.set_index("month")["total_volume"]
        ax.fill_between(mv.index, mv.values, alpha=0.25, color=BLUE)
        ax.plot(mv.index, mv.values, color=BLUE, lw=1.8)
        rm = mv.rolling(3).mean()
        ax.plot(rm.index, rm.values, color=GOLD, lw=1.4, linestyle="--", label="3-mo avg")
        ax.yaxis.set_major_formatter(mill)
        ax.tick_params(axis="x", labelsize=7, colors=SUB)
        ax.legend(fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    save(fig, "02_monthly_volume.png")

    # ③ volume vs revenue share ───────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 5), facecolor=BG)
    style_ax(ax, "Volume Share vs Revenue Share — by Rarity", ylabel="Share (%)")
    if not rs.empty:
        labels3 = rs["rarity"].tolist()
        x3  = np.arange(len(labels3))
        w3  = 0.35
        c3  = [RARITY_COLORS.get(r, SUB) for r in labels3]
        ax.bar(x3 - w3/2, rs["vol_pct"], w3, color=c3, alpha=0.5, label="Volume %")
        bars_r3 = ax.bar(x3 + w3/2, rs["rev_pct"], w3, color=c3, alpha=1.0,
                         edgecolor=BG, linewidth=0.5, label="Revenue %")
        ax.set_xticks(x3)
        ax.set_xticklabels(labels3, rotation=20, ha="right", fontsize=9, color=SUB)
        ax.legend(fontsize=9, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
        for bar in bars_r3:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.3,
                    f"{h:.1f}%", ha="center", va="bottom", color=TEXT, fontsize=8)
    save(fig, "03_rarity_share.png")

    # ④ price range pie ───────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 7), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.set_title("Listing Price-Range Distribution",
                 color=TEXT, fontsize=13, fontweight="bold", pad=12)
    if not price_range_df.empty:
        pie_c = [BLUE, GREEN, GOLD, ORANGE, PURPLE, RED][:len(price_range_df)]
        wedges, texts, autos = ax.pie(
            price_range_df["listing_count"],
            labels=price_range_df["price_range"],
            colors=pie_c, autopct="%1.0f%%", startangle=140, pctdistance=0.75,
            wedgeprops=dict(edgecolor=BG, linewidth=1.5))
        for t in texts:  t.set_color(SUB);  t.set_fontsize(9)
        for a in autos:  a.set_color(BG);   a.set_fontsize(8); a.set_fontweight("bold")
    save(fig, "04_price_range_dist.png")

    # ⑤ avg price per wear ────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(9, 5), facecolor=BG)
    style_ax(ax, "Avg Price by Wear Condition", xlabel="Avg Price (USD)")
    if not wear_price.empty:
        wp = wear_price.copy()
        order = [w for w in WEAR_ORDER if w in wp["wear"].values]
        if order:
            wp = wp.set_index("wear").loc[order].reset_index()
        c5 = [WEAR_COLORS.get(w, SUB) for w in wp["wear"]]
        bars5 = ax.barh(wp["wear"], wp["avg_price"], color=c5, edgecolor=BG, height=0.6)
        for bar, val in zip(bars5, wp["avg_price"]):
            ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                    f"${val:,.2f}", va="center", color=TEXT, fontsize=9)
        ax.set_xlim(0, wp["avg_price"].max() * 1.25)
        ax.tick_params(axis="y", labelsize=9, colors=TEXT)
    save(fig, "05_wear_price.png")

    # ⑥ weapon bubble chart ───────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6), facecolor=BG)
    style_ax(ax, "Weapon Type — Volume vs Avg Price",
             xlabel="Total Volume", ylabel="Avg Price (USD)")
    if not weapon_bubble.empty:
        wb = weapon_bubble[
            weapon_bubble["weapon_type"].notna() &
            (weapon_bubble["weapon_type"].str.strip() != "") &
            (weapon_bubble["weapon_type"].str.lower() != "unknown")
        ].reset_index(drop=True)
        bcolors = [BLUE, GREEN, GOLD, PURPLE, ORANGE, RED, "#80deea", SUB,
                   "#f48fb1", "#bcaaa4"]
        max_vol = wb["total_volume"].max()
        for i, row in wb.iterrows():
            sz  = max(15, (row["total_volume"] / max_vol) * 2000)
            col = bcolors[i % len(bcolors)]
            ax.scatter(row["total_volume"], row["avg_price"],
                       s=sz, color=col, alpha=0.85, edgecolors=BG, linewidths=0.8)
            ax.text(row["total_volume"] * 1.02, row["avg_price"],
                    row["weapon_type"], color=TEXT, fontsize=8, va="center")
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(usd)
        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x:,.0f}"))
    save(fig, "06_weapon_bubble.png")

    # ⑦ top 10 weapons ────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6), facecolor=BG)
    style_ax(ax, "Top 10 Items by Avg Listing Price", xlabel="Avg Price (USD)")
    if not top10.empty:
        names7  = top10["weapon_name"].tolist()
        prices7 = top10["avg_price"].tolist()
        c7 = [GOLD if any(k in n for k in ("★", "Knife", "Glove", "Karambit",
              "Butterfly", "Bayonet", "Flip", "Gut ", "Falchion", "Shadow",
              "Navaja", "Stiletto", "Ursus", "Talon", "Skeleton", "Nomad",
              "Paracord", "Survival")) else BLUE for n in names7]
        bars7 = ax.barh(names7[::-1], prices7[::-1], color=c7[::-1],
                        edgecolor=BG, height=0.65)
        for bar, val in zip(bars7, prices7[::-1]):
            ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                    f"${val:,.0f}", va="center", color=TEXT, fontsize=9)
        ax.set_xlim(0, max(prices7) * 1.2)
        ax.tick_params(axis="y", labelsize=9, colors=TEXT)
    save(fig, "07_top10_weapons.png")
    # ⑦ top 10 weapons, no knives or gloves ────────────────────────────────────────────────────────
  
    fig, ax = plt.subplots(figsize=(10, 6), facecolor=BG)
    style_ax(ax, "Top 10 Items by Avg Listing Price (excl. Knives & Gloves)", xlabel="Avg Price (USD)")
    if not top10_filtered.empty:
        names7  = top10_filtered["weapon_name"].tolist()
        prices7 = top10_filtered["avg_price"].tolist()
        bars7 = ax.barh(names7[::-1], prices7[::-1], color=BLUE,
                        edgecolor=BG, height=0.65)
        for bar, val in zip(bars7, prices7[::-1]):
            ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                    f"${val:,.0f}", va="center", color=TEXT, fontsize=9)
        ax.set_xlim(0, max(prices7) * 1.2)
        ax.tick_params(axis="y", labelsize=9, colors=TEXT)
    else:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center",
                color=SUB, fontsize=11, transform=ax.transAxes)
    save(fig, "07_top10_weapons_filtered.png")

    # ⑧ container age scatter (optional) ──────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 5), facecolor=BG)
    style_ax(ax, "Container Age vs Average Skin Price Multiplier" +
             ("  (no container data)" if not has_container else ""),
             xlabel="Days Since Release", ylabel="Price Multiplier")
    ax.set_yscale("log")
    if has_container and not container_df.empty:
        ctype_c = {"Case": GOLD, "Souvenir Package": PURPLE,
                   "Package": BLUE}
        for ctype, grp in container_df.groupby("container_type"):
            ax.scatter(grp["age_days"], grp["multiplier"],
                       color=ctype_c.get(ctype, SUB), s=55, alpha=0.8,
                       edgecolors=BG, linewidths=0.5, label=ctype)
        z  = np.polyfit(container_df["age_days"], container_df["multiplier"], 1)
        xs = np.linspace(container_df["age_days"].min(), container_df["age_days"].max(), 200)
        ax.plot(xs, np.poly1d(z)(xs), color=TEXT, lw=1.5, linestyle="--",
                alpha=0.5, label="Linear trend")
        ax.axhline(1.0, color=SUB, lw=0.8, linestyle=":")
        ax.text(xs[-1], 1.06, "break-even", color=SUB, fontsize=8, ha="right")
        ax.legend(fontsize=9, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
        ax.yaxis.set_major_formatter(mult)
    else:
        ax.text(0.5, 0.5,
                "No container FK on fact_marketprice\n(add container_id to enable this chart)",
                ha="center", va="center", color=SUB, fontsize=11,
                transform=ax.transAxes)
    save(fig, "08_container_age.png")

    # ⑨ rarity × wear heatmap ─────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(9, 6), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.set_title("Avg Price Heatmap — Rarity × Wear",
                 color=TEXT, fontsize=13, fontweight="bold", pad=12)
    if not pivot.empty:
        im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto")
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels(
            [w.replace("Factory New","FN").replace("Minimal Wear","MW")
              .replace("Field-Tested","FT").replace("Well-Worn","WW")
              .replace("Battle-Scarred","BS") for w in pivot.columns],
            color=SUB, fontsize=9)
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index, color=SUB, fontsize=9)
        vmax = pivot.values.max()
        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                val = pivot.values[i, j]
                if np.isnan(val): continue
                txt    = f"${val:,.0f}" if val >= 10 else f"${val:.2f}"
                tcolor = "black" if val > vmax * 0.6 else TEXT
                ax.text(j, i, txt, ha="center", va="center",
                        fontsize=8, color=tcolor, fontweight="bold")
        cb = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cb.ax.yaxis.set_tick_params(color=SUB, labelsize=7.5)
        cb.ax.set_ylabel("Avg USD", color=SUB, fontsize=8)
    save(fig, "09_heatmap.png")
    # ⑨ rarity × wear heatmap no contraband ─────────────────────────────────────────────────
    
    fig, ax = plt.subplots(figsize=(9, 6), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.set_title("Avg Price Heatmap — Rarity × Wear",
                 color=TEXT, fontsize=13, fontweight="bold", pad=12)
    pivot_no_contraband = pivot.drop(index="Contraband", errors="ignore")
    if not pivot_no_contraband.empty:
        im = ax.imshow(pivot_no_contraband.values, cmap="YlOrRd", aspect="auto")
        ax.set_xticks(range(len(pivot_no_contraband.columns)))
        ax.set_xticklabels(
            [w.replace("Factory New","FN").replace("Minimal Wear","MW")
              .replace("Field-Tested","FT").replace("Well-Worn","WW")
              .replace("Battle-Scarred","BS") for w in pivot_no_contraband.columns],
            color=SUB, fontsize=9)
        ax.set_yticks(range(len(pivot_no_contraband.index)))
        ax.set_yticklabels(pivot_no_contraband.index, color=SUB, fontsize=9)
        vmax = pivot_no_contraband.values.max()
        for i in range(len(pivot_no_contraband.index)):
            for j in range(len(pivot_no_contraband.columns)):
                val = pivot_no_contraband.values[i, j]
                if np.isnan(val): continue
                txt    = f"${val:,.0f}" if val >= 10 else f"${val:.2f}"
                tcolor = "black" if val > vmax * 0.6 else TEXT
                ax.text(j, i, txt, ha="center", va="center",
                        fontsize=8, color=tcolor, fontweight="bold")
        cb = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cb.ax.yaxis.set_tick_params(color=SUB, labelsize=7.5)
        cb.ax.set_ylabel("Avg USD", color=SUB, fontsize=8)
    save(fig, "09_heatmap_no_contraband.png")

    print(f"\n  All charts saved to: {os.path.abspath(OUTPUT_DIR)}/\n")


if __name__ == "__main__":
    main()