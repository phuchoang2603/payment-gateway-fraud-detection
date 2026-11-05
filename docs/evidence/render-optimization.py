import json
import pathlib
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = pathlib.Path.cwd()
plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.facecolor": "#f5f7fb",
        "axes.facecolor": "#f5f7fb",
        "text.color": "#18253a",
        "axes.labelcolor": "#18253a",
        "savefig.facecolor": "#f5f7fb",
    }
)
BEFORE = "#94a3b8"
AFTER = "#007f78"


def read(p):
    return json.loads((ROOT / p).read_text())


def finish(fig, path, title, subtitle, footer):
    fig.suptitle(title, x=0.055, ha="left", y=0.97, fontsize=20, fontweight="bold")
    fig.text(0.055, 0.885, subtitle, fontsize=10.5)
    fig.text(0.055, 0.035, footer, fontsize=9, color="#46566d")
    fig.subplots_adjust(
        top=0.77, bottom=0.19, left=0.095, right=0.94, wspace=0.48, hspace=0.65
    )
    fig.savefig(ROOT / path, dpi=170, bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)


def bars(ax, values, labels, unit, title):
    ax.barh([1, 0], values, color=[BEFORE, AFTER], height=0.48)
    ax.set_yticks([1, 0], labels)
    ax.set_xlim(0, max(values) * 1.38)
    ax.set_title(title, loc="left", fontweight="bold", pad=12)
    ax.set_xlabel(unit)
    ax.grid(axis="x", alpha=0.12)
    ax.set_axisbelow(True)
    for y, v in zip([1, 0], values):
        ax.text(
            v + max(values) * 0.025,
            y,
            (f"{v:,.0f}" if v >= 100 else f"{v:,.3g}"),
            va="center",
            fontweight="bold",
        )


proof = read("docs/deployment/evidence/image-size-proof.json")["measurements"]
p = {r["name"]: r for r in proof}
fig, axs = plt.subplots(1, 2, figsize=(12, 5.2))
v = [
    p["streaming-before"]["compressed_layer_bytes"] / 1e9,
    p["streaming-after"]["compressed_layer_bytes"] / 1e9,
]
bars(
    axs[0],
    v,
    ["Before 6cec4a9", "After 52219c0"],
    "Compressed layer bytes (decimal GB)",
    f"Streaming: {(1 - v[1] / v[0]) * 100:.1f}% smaller",
)
axs[1].axis("off")
axs[1].text(0, 1, "Registry evidence", va="top", fontsize=14, fontweight="bold")
lines = [
    "linux/amd64; 17 layers in each image",
    f"Before: {p['streaming-before']['compressed_layer_bytes']:,} bytes",
    f"After:    {p['streaming-after']['compressed_layer_bytes']:,} bytes",
    "",
    "Before manifest: 9b502b7a822f…",
    "After manifest:    268b0af6b563…",
    "",
    "docker buildx imagetools inspect --raw",
    "Sum layers[].size from platform manifests.",
    "Config and attestations excluded.",
]
axs[1].text(0, 0.82, "\n".join(lines), va="top", fontsize=10.5, linespacing=1.6)
finish(
    fig,
    "docs/deployment/images/docker-streaming-before-after.png",
    "Docker image optimization: registry-verified before / after",
    "Historical Python streaming packaging change, 2026-10-27. Registry manifests rechecked on 2026-11-05.",
    "Source: deployment/evidence/image-size-proof.json + platform manifests. Image size, not build speed or JVM throughput.",
)
s = read("docs/deployment/evidence/spark-historical-sizes.json")
fig, axs = plt.subplots(1, 2, figsize=(12, 5))
for ax, key, title in zip(
    axs, ["compressed_GB", "local_GB"], ["Registry compression", "Local image size"]
):
    vals = [s[key]["before"], s[key]["after"]]
    bars(
        ax,
        vals,
        ["Before 1e090ee", "After opt"],
        "Decimal GB",
        f"{title}: {(1 - vals[1] / vals[0]) * 100:.1f}% smaller",
    )
finish(
    fig,
    "docs/deployment/images/docker-spark-before-after.png",
    "Spark image optimization: recorded measurements",
    "2026-10-22; uv cache removed and installed dependencies copied into the runtime.",
    "Source: deployment/evidence/spark-historical-sizes.json. Redrawn from archived rounded values; original images unavailable.",
)
fig, axs = plt.subplots(2, 1, figsize=(14, 6.8))
for ax, side in zip(axs, ["before", "after"]):
    ax.axis("off")
    txt = (
        (ROOT / f"docs/deployment/evidence/spark-build-{side}.txt")
        .read_text()
        .split("\n\n", 1)[1]
    )
    txt = "\n".join(
        textwrap.fill(line, 145, subsequent_indent="    ") for line in txt.splitlines()
    )
    ax.text(
        0,
        1,
        side.upper() + " — retained build output",
        va="top",
        weight="bold",
        fontsize=12,
    )
    ax.text(
        0,
        0.76,
        txt,
        va="top",
        family="DejaVu Sans Mono",
        fontsize=8.7,
        linespacing=1.55,
    )
finish(
    fig,
    "docs/deployment/images/docker-spark-build-proof.png",
    "Spark builds: successful exports before / after",
    "Actual retained BuildKit log excerpts, rendered for documentation; these are not terminal screenshots.",
    "Source: deployment/evidence/spark-build-{before,after}.txt. Export success does not establish a build-time speedup.",
)
z = read("docs/data/evidence/zorder-2026-11-01.json")
fig, axs = plt.subplots(2, 2, figsize=(12, 8))
b = z["bronze_one_day"]
bars(
    axs[0, 0],
    [b["no_filter"]["files_planned"], b["one_day"]["files_planned"]],
    ["No date predicate", "One-day predicate"],
    "Files planned",
    "Partition pruning: 99.86% fewer files",
)
bars(
    axs[0, 1],
    [b["no_filter"]["bytes_planned"] / 1e9, b["one_day"]["bytes_planned"] / 1e9],
    ["No date predicate", "One-day predicate"],
    "Decimal GB planned",
    "Partition pruning: 99.75% fewer bytes",
)
bars(
    axs[1, 0],
    [z["before"]["files_planned"], z["after"]["files_planned"]],
    ["Before Z-order", "After Z-order"],
    "Files planned",
    "Customer filter: no files skipped",
)
bars(
    axs[1, 1],
    [z["before"]["bytes_planned"] / 1e9, z["after"]["bytes_planned"] / 1e9],
    ["Before Z-order", "After Z-order"],
    "Decimal GB planned",
    "Z-order: planned bytes increased 0.98%",
)
finish(
    fig,
    "docs/data/images/lakehouse-optimization-before-after.png",
    "Lakehouse: pruning gain and a measured negative result",
    "Recorded AWS v19, 2026-11-01; maintain-lakehouse-zorder-8z67x. Z-order rewrite took 69.57 seconds.",
    "Source: data/evidence/zorder-2026-11-01.json. Planned files/bytes, not query latency. Date filtering changes the selected data range.",
)
w = read("docs/data/evidence/warehouse-comparison.json")
fig, axs = plt.subplots(1, 3, figsize=(14, 5.6))
for ax, q in zip(axs, w["queries"]):
    change = (q["after_ms"] / q["before_ms"] - 1) * 100
    bars(
        ax,
        [q["before_ms"], q["after_ms"]],
        ["Index disabled", "Index enabled"],
        "Query time (ms)",
        q["name"] + f": {change:+.1f}% time",
    )
    ax.set_xlabel(
        f"Query time (ms)\nGranules: {q['before_granules']:,} → {q['after_granules']:,}"
    )
finish(
    fig,
    "docs/data/images/warehouse-index-before-after.png",
    "Warehouse skip indexes: benefit depends on selectivity",
    "Recorded AWS v19; 44.2M rows; warm runs, query and query-condition caches off. Q3 is 12.06× faster.",
    "Source: data/evidence/warehouse-comparison.json, transcribed from storage.md@fc197dbc5c9c. Historical single-run values, not new captures.",
)
fig, axs = plt.subplots(1, 3, figsize=(14, 5.3))
for ax, key, title in zip(
    axs[:2],
    ["uuid_string", "fingerprint_string"],
    ["UUID string", "Fingerprint string"],
):
    c = w["codecs"][key]
    bars(
        ax,
        [c["before_bytes_per_row"], c["after_bytes_per_row"]],
        ["Before codec", "ZSTD(3)"],
        "Stored bytes per row",
        title,
    )
bars(
    axs[2],
    [w["daily_kpi"]["fact_ms"], w["daily_kpi"]["mart_ms"]],
    ["Attempt fact", "Daily mart"],
    "Query time (ms)",
    "Daily KPIs: 28.3× faster",
)
finish(
    fig,
    "docs/data/images/warehouse-codec-mart-before-after.png",
    "Warehouse layout: smaller IDs and pre-aggregated KPIs",
    "Codec comparison: 2M-row sample. KPI comparison: recorded same hot merchant/month; different query shapes.",
    "Source: data/evidence/warehouse-comparison.json. Redrawn historical values; no claim of a current GCP rerun.",
)
