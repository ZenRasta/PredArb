from __future__ import annotations
import csv, os
from typing import Set, List
from app.grouping import compute_group_for_seed

def _split_ids(s: str) -> Set[str]:
    if not s: return set()
    return set([x.strip() for x in s.split("|") if x.strip()])

def test_grouping_precision_recall(capsys):
    labels_path = os.path.join(os.path.dirname(__file__), "..", "eval", "grouping_labels.csv")
    seeds = []
    with open(labels_path, newline="") as f:
        for row in csv.DictReader(f):
            seeds.append(row)

    tp = fp = fn = 0
    for row in seeds:
        seed = row["seed_market_id"]
        positives = _split_ids(row["positive_ids"])
        negatives = _split_ids(row["negative_ids"])
        pred = set(compute_group_for_seed(seed))
        # remove seed itself for scoring clarity
        pred.discard(seed)

        tp += len(pred & positives)
        fp += len(pred & negatives)
        fn += len(positives - pred)

    precision = tp / (tp + fp) if (tp+fp) else 1.0
    recall    = tp / (tp + fn) if (tp+fn) else 1.0

    print(f"Precision={precision:.3f} Recall={recall:.3f} (TP={tp}, FP={fp}, FN={fn})")
    # Targets from your acceptance criteria
    assert precision >= 0.95
    assert recall >= 0.90

