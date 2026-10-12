"""Train the severity classifier (syllabus module 5) on real Loghub logs.

Run `python -m spark.prepare_dataset` first. It builds the labelled training set from
the published Loghub datasets (see that module for sources and labelling rules).

Pipeline:
    tokenize -> remove stop words -> TF (hashing) -> IDF                    -> "tfidf"
                                  -> word bigrams -> TF (hashing) -> IDF    -> "tfidf_bigrams"
    [tfidf, tfidf_bigrams] -> features -> classifier -> High / Medium / Low

The stream job reuses the fitted pipeline twice: the unigram "tfidf" vectors drive the
cosine-similarity clustering, and the classifier grades each alert. Several classifiers
are compared in `spark/evaluate_model.py`; MODEL picks the one that is saved.

    python -m spark.train_model
"""

import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LinearSVC, LogisticRegression, MultilayerPerceptronClassifier,
    NaiveBayes, OneVsRest, RandomForestClassifier,
)
from pyspark.ml.feature import (
    HashingTF, IDF, IndexToString, NGram, RegexTokenizer, StopWordsRemover, StringIndexer,
    UnivariateFeatureSelector, VectorAssembler,
)
from pyspark.sql import DataFrame, functions as F

from logiq import config
from spark.common import local_uri, spark_session

LABELS = sorted(["High", "Medium", "Low"])
HASH_FEATURES = 1 << 14
# Trees and the neural network are slow on 32,768 sparse columns (Spark densifies them
# for the network and bins every column for trees), so they get the top columns by ANOVA F-test.
SELECTED_FEATURES = 2000

# name -> (factory, outputs class probabilities, needs feature selection). The stream
# needs probabilities for the confidence it shows, so only those models can be saved.
CLASSIFIERS = {
    "naive_bayes": (lambda: NaiveBayes(modelType="multinomial", smoothing=1.0), True, False),
    "logistic_regression": (lambda: LogisticRegression(
        family="multinomial", weightCol="weight", regParam=0.001, elasticNetParam=0.5, maxIter=100), True, False),
    "linear_svm": (lambda: OneVsRest(
        weightCol="weight", classifier=LinearSVC(regParam=0.01, maxIter=50)), False, False),
    "random_forest": (lambda: RandomForestClassifier(
        weightCol="weight", numTrees=100, maxDepth=15, seed=42), True, True),
    "neural_network": (lambda: MultilayerPerceptronClassifier(
        layers=[SELECTED_FEATURES, 64, len(LABELS)], maxIter=100, seed=42), True, True),
}
MODEL = os.environ.get("SEVERITY_MODEL", "logistic_regression")


def build_pipeline(name: str = MODEL) -> Pipeline:
    factory, _, select = CLASSIFIERS[name]
    classifier = factory().setFeaturesCol("selected" if select else "features").setLabelCol("label")
    selector = [UnivariateFeatureSelector(
        featuresCol="features", outputCol="selected", labelCol="label", selectionMode="numTopFeatures",
    ).setFeatureType("continuous").setLabelType("categorical").setSelectionThreshold(SELECTED_FEATURES)] if select else []
    return Pipeline(stages=[
        StringIndexer(inputCol="severity", outputCol="label", stringOrderType="alphabetAsc"),
        # Letters only: numbers, ids and IPs carry no meaning for similarity or severity.
        RegexTokenizer(inputCol="message", outputCol="tokens", pattern="[^a-zA-Z]+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="words"),
        HashingTF(inputCol="words", outputCol="tf", numFeatures=HASH_FEATURES),
        IDF(inputCol="tf", outputCol="tfidf"),
        NGram(n=2, inputCol="words", outputCol="bigrams"),
        HashingTF(inputCol="bigrams", outputCol="tf_bigrams", numFeatures=HASH_FEATURES),
        IDF(inputCol="tf_bigrams", outputCol="tfidf_bigrams"),
        VectorAssembler(inputCols=["tfidf", "tfidf_bigrams"], outputCol="features"),
        *selector,
        classifier,
        IndexToString(inputCol="prediction", outputCol="predicted_severity", labels=LABELS),
    ])


def load_dataset(spark) -> DataFrame:
    """The Loghub training set, with balanced class weights (Medium is ~78% of rows)."""
    data = spark.read.parquet(local_uri(config.TRAINING_DATA))
    counts = {r["severity"]: r["count"] for r in data.groupBy("severity").count().collect()}
    total = sum(counts.values())
    weights = F.create_map(*[x for c, n in counts.items() for x in (F.lit(c), F.lit(total / (len(counts) * n)))])
    return data.withColumn("weight", weights[F.col("severity")])


def main() -> None:
    if not CLASSIFIERS[MODEL][1]:
        raise SystemExit(f"{MODEL} has no class probabilities, which the stream needs. Pick another SEVERITY_MODEL.")
    spark = spark_session("logiq-train")
    data = load_dataset(spark).cache()
    print(f"Training {MODEL} on {data.count()} Loghub rows ...")
    build_pipeline(MODEL).fit(data).write().overwrite().save(local_uri(config.MODEL_PATH))
    print(f"Model saved to {config.MODEL_PATH}. Run `python -m spark.evaluate_model` for metrics.")
    spark.stop()


if __name__ == "__main__":
    main()
