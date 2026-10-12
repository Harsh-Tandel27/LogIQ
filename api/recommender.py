"""Content-based fix recommendations (syllabus module 6).

Each runbook (past incident + fix) is a TF-IDF item profile built from its
service, title and symptoms. An incident's profile comes from its root cause
and its alert messages. Runbooks are ranked by cosine similarity to the incident.
Fixes written when an engineer resolves an incident become new runbooks.
"""

import json

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from api.db import db
from logiq import config


def seed_runbooks() -> None:
    if db().runbooks.count_documents({}) == 0:
        runbooks = json.loads(config.RUNBOOK_FILE.read_text())
        db().runbooks.insert_many([{**rb, "_id": rb["id"], "source": "seed"} for rb in runbooks])


def _profile(runbook: dict) -> str:
    # The service name counts twice: the same service is a strong signal.
    return f"{runbook['service']} {runbook['service']} {runbook['title']} {runbook['symptoms']}"


def recommend(root_cause: str, messages: list[str], top_n: int = 3) -> list[dict]:
    runbooks = list(db().runbooks.find())
    if not runbooks:
        return []
    vectorizer = TfidfVectorizer(token_pattern=r"[A-Za-z]{2,}", stop_words="english", sublinear_tf=True)
    item_matrix = vectorizer.fit_transform([_profile(rb) for rb in runbooks])
    query = vectorizer.transform([f"{root_cause} {root_cause} " + " ".join(messages)])
    scores = cosine_similarity(query, item_matrix)[0]
    ranked = sorted(zip(scores, runbooks), key=lambda pair: pair[0], reverse=True)
    return [
        {"id": rb["_id"], "title": rb["title"], "service": rb["service"], "fix": rb["fix"], "score": round(float(score), 3)}
        for score, rb in ranked[:top_n]
        if score > 0.05
    ]
