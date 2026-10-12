"""Compare severity classifiers on real Loghub logs and save a metrics report.

Test setup: 5-fold cross-validation grouped by message family. Every fold holds out whole
families of templates (see spark/prepare_dataset.py), so each score reflects wording the
model has never seen. Classes are imbalanced (Medium is ~78% of rows), so models are
ranked by macro F1, which weights High, Medium and Low equally.

A second check scores the LogIQ simulator's own error messages. They come from a different
system and use a different labelling idea, so this measures domain transfer only.

    python -m spark.evaluate_model                                    # every classifier
    python -m spark.evaluate_model logistic_regression naive_bayes    # a subset
"""

import json
import random
import sys
import time

from pyspark.sql import functions as F
from sklearn.metrics import accuracy_score, classification_report, cohen_kappa_score, confusion_matrix

from logiq import config, templates
from spark.common import spark_session
from spark.prepare_dataset import FOLDS
from spark.train_model import CLASSIFIERS, MODEL, build_pipeline, load_dataset

LABELS = ["High", "Medium", "Low"]


def scores(y_true, y_pred) -> dict:
    report = classification_report(y_true, y_pred, labels=LABELS, output_dict=True, zero_division=0)
    return {
        "rows": len(y_true),
        "accuracy": accuracy_score(y_true, y_pred),
        "macro": {k: report["macro avg"][k] for k in ("precision", "recall", "f1-score")},
        "weighted": {k: report["weighted avg"][k] for k in ("precision", "recall", "f1-score")},
        "cohen_kappa": cohen_kappa_score(y_true, y_pred),
        "per_class": {c: {k: report[c][k] for k in ("precision", "recall", "f1-score", "support")} for c in LABELS},
        "confusion_matrix": {"labels": LABELS, "rows_true_cols_pred": confusion_matrix(y_true, y_pred, labels=LABELS).tolist()},
    }


def cross_validate(data, name: str) -> dict:
    started = time.time()
    y_true, y_pred = [], []
    for k in range(FOLDS):
        model = build_pipeline(name).fit(data.filter(F.col("fold") != k))
        scored = model.transform(data.filter(F.col("fold") == k)).select("severity", "predicted_severity").collect()
        y_true += [r["severity"] for r in scored]
        y_pred += [r["predicted_severity"] for r in scored]
    return {**scores(y_true, y_pred), "seconds": round(time.time() - started)}


def simulator_transfer(spark, data, name: str) -> dict:
    rows = templates.training_rows(random.Random(42), per_template=10)
    sim = spark.createDataFrame([(m, s) for m, s, _ in rows], ["message", "severity"])
    scored = build_pipeline(name).fit(data).transform(sim).select("severity", "predicted_severity").collect()
    return scores([r["severity"] for r in scored], [r["predicted_severity"] for r in scored])


def print_result(name: str, r: dict) -> None:
    print(f"\n=== {name}: {r['rows']} rows ===")
    print(f"accuracy        {r['accuracy']:.3f}")
    print(f"macro     P {r['macro']['precision']:.3f}  R {r['macro']['recall']:.3f}  F1 {r['macro']['f1-score']:.3f}")
    print(f"weighted  P {r['weighted']['precision']:.3f}  R {r['weighted']['recall']:.3f}  F1 {r['weighted']['f1-score']:.3f}")
    print(f"Cohen's kappa   {r['cohen_kappa']:.3f}")
    print(f"{'class':<8}{'precision':>10}{'recall':>8}{'F1':>8}{'support':>9}")
    for c, m in r["per_class"].items():
        print(f"{c:<8}{m['precision']:>10.3f}{m['recall']:>8.3f}{m['f1-score']:>8.3f}{int(m['support']):>9}")
    print("confusion matrix (rows = true, cols = predicted: High, Medium, Low)")
    for label, row in zip(LABELS, r["confusion_matrix"]["rows_true_cols_pred"]):
        print(f"  {label:<7}{row}")


def main() -> None:
    names = sys.argv[1:] or list(CLASSIFIERS)
    spark = spark_session("logiq-evaluate")
    data = load_dataset(spark).cache()

    results = {}
    for name in names:
        print(f"Cross-validating {name} ...", flush=True)
        results[name] = cross_validate(data, name)
        print_result(f"{name}, {FOLDS}-fold grouped CV", results[name])

    print(f"\n{'model':<24}{'accuracy':>9}{'macro F1':>10}{'kappa':>8}{'seconds':>9}")
    for name, r in sorted(results.items(), key=lambda kv: -kv[1]["macro"]["f1-score"]):
        print(f"{name:<24}{r['accuracy']:>9.3f}{r['macro']['f1-score']:>10.3f}{r['cohen_kappa']:>8.3f}{r['seconds']:>9}")

    transfer = simulator_transfer(spark, data, MODEL)
    print_result(f"{MODEL} trained on all Loghub rows, scored on LogIQ simulator messages", transfer)

    sources = {r["source"]: r["count"] for r in data.groupBy("source").count().collect()}
    report = {
        "dataset": {
            "name": "Loghub (Zhu et al., ISSRE 2023), doi:10.5281/zenodo.8196385",
            "rows": data.count(),
            "rows_by_source": sources,
            "class_counts": {r["severity"]: r["count"] for r in data.groupBy("severity").count().collect()},
            "evaluation": f"{FOLDS}-fold cross-validation, folds grouped by message family",
        },
        "features": "HashingTF 16384 + IDF on words, and on word bigrams, concatenated",
        "saved_model": MODEL,
        "cross_validation": results,
        "simulator_transfer": {"model": MODEL, **transfer},
    }
    out = config.ROOT / "models" / "metrics.json"
    if out.exists() and sys.argv[1:]:
        old = json.loads(out.read_text())
        report["cross_validation"] = {**old.get("cross_validation", {}), **results}
    out.write_text(json.dumps(report, indent=2))
    print(f"\nSaved {out}")
    spark.stop()


if __name__ == "__main__":
    main()
