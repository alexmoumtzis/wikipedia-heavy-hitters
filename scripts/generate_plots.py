"""
Generate all four benchmark figures for the LaTeX report.
Run from the repo root:  python scripts/generate_plots.py
Output: figures/fig_baseline.pdf, fig_f1_memory.pdf, fig_skew.pdf, fig_distributed.pdf
"""

import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

os.makedirs('figures', exist_ok=True)

# Consistent style
plt.rcParams.update({
    'font.size': 10,
    'axes.titlesize': 11,
    'axes.labelsize': 10,
    'legend.fontsize': 9,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
})

COLORS = {
    'CMS':       '#1f77b4',
    'FastAMS':   '#d62728',
    'MG':        '#2ca02c',
    'SS':        '#ff7f0e',
    'LC':        '#9467bd',
    'Concise':   '#8c564b',
    'Reservoir': '#17becf',
}
MARKERS = {'CMS': 'o', 'FastAMS': 's', 'MG': '^', 'SS': 'D',
           'LC': '*', 'Concise': 'x', 'Reservoir': '+'}
LINES   = {'CMS': '-', 'FastAMS': '-',
           'MG': '--', 'SS': '--', 'LC': '--',
           'Concise': ':', 'Reservoir': ':'}

ALGOS = ['CMS', 'FastAMS', 'MG', 'SS', 'LC', 'Concise', 'Reservoir']

def grid(ax):
    ax.yaxis.grid(True, linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)

# Figure 1: baseline precision / recall / throughput
precision  = [98.1, 98.1, 92.8, 96.3, 90.6, 77.1, 75.5]
recall     = [98.1, 98.1, 98.1, 98.1, 98.1, 93.7, 89.9]
throughput = [196132, 78109, 267600, 145786, 328208, 351929, 442106]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
x  = np.arange(len(ALGOS))
bc = [COLORS[a] for a in ALGOS]

ax1.bar(x, precision, color=bc, edgecolor='#333', linewidth=0.5, alpha=0.88, zorder=3)
ax1.plot(x, recall, 'k--', marker='o', markersize=4, linewidth=1.5,
         label='Recall', zorder=4)
ax1.set_xticks(x); ax1.set_xticklabels(ALGOS, rotation=30, ha='right')
ax1.set_ylabel('Accuracy (%)'); ax1.set_ylim(65, 103)
ax1.set_title('(a) Precision & Recall')
ax1.legend(loc='lower left'); grid(ax1)

ax2.bar(x, [t / 1000 for t in throughput],
        color=bc, edgecolor='#333', linewidth=0.5, alpha=0.88, zorder=3)
ax2.set_xticks(x); ax2.set_xticklabels(ALGOS, rotation=30, ha='right')
ax2.set_ylabel('Throughput (×1 000 rows/s)')
ax2.set_title('(b) Throughput'); grid(ax2)

patches = [mpatches.Patch(color=COLORS[a], label=a) for a in ALGOS]
fig.legend(handles=patches, loc='lower center', ncol=7, fontsize=8,
           bbox_to_anchor=(0.5, -0.07))
fig.tight_layout()
fig.savefig('figures/fig_baseline.pdf', bbox_inches='tight')
plt.close(fig)
print("✓ figures/fig_baseline.pdf")

# Figure 2: F1 vs memory budget
MEM_KB = [128, 256, 512, 1024, 2048, 4096, 8192]
F1 = {
    'CMS':       [0.0,  16.3, 75.2, 85.2, 96.6, 98.7, 99.7],
    'FastAMS':   [69.1, 96.0, 99.4, 99.7,100.0,100.0,100.0],
    'MG':        [51.0, 65.5, 81.2, 88.7, 95.0, 98.1, 99.0],
    'SS':        [12.9, 11.1,  7.7, 73.5, 91.8, 96.3, 98.4],
    'LC':        [45.8, 61.4, 83.9, 99.3,100.0,100.0,100.0],
    'Concise':   [8.2,   6.0, 57.4, 66.9, 77.8, 85.5, 91.1],
    'Reservoir': [7.2,  60.4, 69.4, 81.6, 85.7, 87.8, 93.0],
}

fig, ax = plt.subplots(figsize=(9, 5))
for algo, f1 in F1.items():
    ax.semilogx(MEM_KB, f1,
                color=COLORS[algo], linestyle=LINES[algo],
                marker=MARKERS[algo], markersize=6,
                linewidth=1.8, label=algo, base=2)

ax.set_xlim(90, 12000); ax.set_ylim(0, 105)
ax.set_xticks(MEM_KB)
ax.set_xticklabels(['128 KB','256 KB','512 KB','1 MB','2 MB','4 MB','8 MB'],
                   rotation=25, ha='right')
ax.set_xlabel('Memory budget'); ax.set_ylabel('$F_1$ (%)')
ax.legend(loc='lower right'); grid(ax)
ax.xaxis.grid(True, linestyle='--', alpha=0.3)
fig.tight_layout()
fig.savefig('figures/fig_f1_memory.pdf', bbox_inches='tight')
plt.close(fig)
print("✓ figures/fig_f1_memory.pdf")

# Figure 3: skew robustness
SKEW_X      = [0, 1, 2, 3, 4, 5]
SKEW_LABELS = ['s=0.6','s=0.8','s=1.0','s=1.2','s=1.5','wiki']
SKEW_F1 = {
    'CMS':       [98, 99,100,100,100, 99],
    'FastAMS':   [98,100,100,100,100,100],
    'MG':        [93, 97, 99,100,100, 96],
    'SS':        [92, 97, 99,100,100, 95],
    'LC':        [100,100,100,100,100,100],
    'Concise':   [81, 88, 92, 94, 91, 81],
    'Reservoir': [81, 93, 96, 96, 95, 86],
}

fig, ax = plt.subplots(figsize=(9, 5))
ax.axvline(x=4.5, color='gray', linestyle='--', linewidth=1, alpha=0.6)
ax.text(4.56, 77.5, '← real data', fontsize=8, color='gray')
for algo, vals in SKEW_F1.items():
    ax.plot(SKEW_X, vals,
            color=COLORS[algo], linestyle=LINES[algo],
            marker=MARKERS[algo], markersize=6,
            linewidth=1.8, label=algo)

ax.set_xticks(SKEW_X); ax.set_xticklabels(SKEW_LABELS)
ax.set_xlabel('Zipf exponent / dataset')
ax.set_ylabel('Top-100 $F_1$ (%)')
ax.set_ylim(76, 103)
ax.legend(loc='lower right'); grid(ax)
fig.tight_layout()
fig.savefig('figures/fig_skew.pdf', bbox_inches='tight')
plt.close(fig)
print("✓ figures/fig_skew.pdf")

# Figure 4: distributed speedup + merge deviation
P = [1, 2, 4, 8, 16]
SPEEDUP = {
    'CMS':     [1.000, 1.766, 3.311, 4.443, 4.389],
    'FastAMS': [1.000, 2.000, 3.024, 3.972, 3.689],
    'MG':      [1.000, 2.269, 4.219, 6.007, 5.908],
}
DEVIATION = {
    'CMS':     [0, 0,    0,    0,    0],
    'FastAMS': [0, 0,    0,    0,    0],
    'MG':      [0, 1777, 2425, 2712, 2823],
}

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

ax1.plot(P, P, 'k--', alpha=0.3, linewidth=1.2, label='Ideal', zorder=1)
for algo, sp in SPEEDUP.items():
    ls = '-' if algo != 'MG' else '--'
    ax1.semilogx(P, sp, color=COLORS[algo], linestyle=ls,
                 marker=MARKERS[algo], markersize=6, linewidth=1.8,
                 label=algo, base=2)
ax1.set_xticks(P); ax1.set_xticklabels(P)
ax1.set_xlabel('Partitions $P$'); ax1.set_ylabel('Speedup vs. P=1')
ax1.set_ylim(0.5, 7.5); ax1.set_title('(a) Parallel speedup')
ax1.legend(); grid(ax1)

for algo, dev in DEVIATION.items():
    ls = '-' if algo != 'MG' else '--'
    ax2.semilogx(P, dev, color=COLORS[algo], linestyle=ls,
                 marker=MARKERS[algo], markersize=6, linewidth=1.8,
                 label=algo, base=2)
ax2.set_xticks(P); ax2.set_xticklabels(P)
ax2.set_xlabel('Partitions $P$')
ax2.set_ylabel('Max abs. deviation (page views)')
ax2.set_ylim(-100, 3200); ax2.set_title('(b) Merge deviation from P=1')
ax2.legend(); grid(ax2)

fig.tight_layout()
fig.savefig('figures/fig_distributed.pdf', bbox_inches='tight')
plt.close(fig)
print("✓ figures/fig_distributed.pdf")

print("\nAll done — upload the figures/ folder to Overleaf.")
