from dataclasses import dataclass

import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score, roc_curve

DECLINE_FPR = 0.001
STEP_UP_FPR = 0.01
MARGIN = 0.005


def threshold_at(y, scores, w, rate):
    fpr, _, thresholds = roc_curve(y, scores, sample_weight=w)
    return float(min(thresholds[max(np.searchsorted(fpr, rate, side="right") - 1, 0)], 2.0))


def recall_at(y, scores, w, rate):
    fpr, tpr, _ = roc_curve(y, scores, sample_weight=w)
    return float(tpr[max(np.searchsorted(fpr, rate, side="right") - 1, 0)])


def measure(y, scores, w, decline):
    flagged = scores >= decline
    positive = y > 0
    true_positive = w[flagged & positive].sum()
    flagged_weight = w[flagged].sum()
    return {
        "pr_auc": float(average_precision_score(y, scores, sample_weight=w)),
        "roc_auc": float(roc_auc_score(y, scores, sample_weight=w)),
        "recall_at_1pct_fpr": recall_at(y, scores, w, STEP_UP_FPR),
        "precision_at_decline": float(true_positive / flagged_weight) if flagged_weight else 0.0,
        "recall_at_decline": float(true_positive / w[positive].sum()),
    }


@dataclass
class Verdict:
    passed: bool
    reasons: list


class GateEvaluator:
    def __init__(self, margin=MARGIN):
        self.margin = margin

    def thresholds(self, y, scores, w):
        return {
            "decline": threshold_at(y, scores, w, DECLINE_FPR),
            "step_up": threshold_at(y, scores, w, STEP_UP_FPR),
        }

    def evaluate(self, thresholds, y, w, candidates):
        return {
            name: measure(y, scores, w, thresholds[name]["decline"])
            for name, scores in candidates.items()
        }

    def gate(self, metrics):
        new, baseline = metrics["model"], metrics["baseline"]
        reasons = []
        if new["pr_auc"] <= baseline["pr_auc"]:
            reasons.append(
                f"test PR-AUC {new['pr_auc']:.4f} does not beat the baseline {baseline['pr_auc']:.4f}"
            )
        production = metrics.get("production")
        if production is not None:
            if new["pr_auc"] < production["pr_auc"] + self.margin:
                reasons.append(
                    f"test PR-AUC {new['pr_auc']:.4f} is under production "
                    f"{production['pr_auc']:.4f} plus the {self.margin} margin"
                )
            if new["recall_at_1pct_fpr"] < production["recall_at_1pct_fpr"]:
                reasons.append(
                    f"recall at 1% FPR {new['recall_at_1pct_fpr']:.4f} is under production "
                    f"{production['recall_at_1pct_fpr']:.4f}"
                )
        return Verdict(not reasons, reasons)


def pr_curve_html(curves, points=200):
    palette = ["#2563eb", "#9ca3af", "#dc2626"]
    paths = []
    for color, (name, (precision, recall)) in zip(palette, curves.items()):
        step = max(len(recall) // points, 1)
        coordinates = " ".join(
            f"{40 + 480 * float(r):.1f},{300 - 280 * float(p):.1f}"
            for r, p in zip(recall[::step], precision[::step])
        )
        paths.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{coordinates}"/>'
        )
    legend = "".join(
        f'<text x="60" y="{30 + 18 * index}" fill="{color}" font-size="13">{name}</text>'
        for index, (color, name) in enumerate(zip(palette, curves))
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="560" height="340" font-family="sans-serif">'
        '<rect x="40" y="20" width="480" height="280" fill="none" stroke="#6b7280"/>'
        f"{''.join(paths)}{legend}"
        '<text x="240" y="330" font-size="13">recall</text>'
        '<text x="4" y="160" font-size="13">precision</text></svg>'
    )
