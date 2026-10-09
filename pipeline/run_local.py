"""
Local (no-Docker) runner for the beginner_de_project batch pipeline.

This reproduces, on plain Python + DuckDB, what the original Airflow +
Spark + MinIO + Postgres stack did:

    1. classify movie reviews (mark reviews mentioning "good" as positive)
    2. load the user purchase table (OnlineRetail.csv)
    3. join purchases with reviews per customer
    4. compute behaviour metrics -> data/behaviour_metrics.csv
    5. render the Plotly dashboard -> dags/scripts/dashboard/dashboard.html

Run it with:

    python pipeline/run_local.py

Everything is written relative to the project root, so it works no matter
which directory you launch it from.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
MOVIE_REVIEW_CSV = DATA_DIR / "movie_review.csv"
ONLINE_RETAIL_CSV = DATA_DIR / "OnlineRetail.csv"
BEHAVIOUR_METRICS_CSV = DATA_DIR / "behaviour_metrics.csv"
DASHBOARD_HTML = ROOT / "dags" / "scripts" / "dashboard" / "dashboard.html"

# Column names from containers/upstream/1-create-user-purchase.sql
USER_PURCHASE_COLUMNS = {
    "InvoiceNo": "invoice_number",
    "StockCode": "stock_code",
    "Description": "detail",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}


def log(msg: str) -> None:
    print(f"[pipeline] {msg}", flush=True)


def classify_reviews() -> pd.DataFrame:
    """Reproduce dags/scripts/spark/random_text_classification.py.

    A review is `positive_review` when the cleaned token list contains the
    word "good" (Tokenizer lowercases + whitespace split, StopWordsRemover
    removes english stop words, then array_contains(..., "good")).
    """
    log(f"reading reviews from {MOVIE_REVIEW_CSV}")
    reviews = pd.read_csv(
        MOVIE_REVIEW_CSV,
        usecols=["cid", "review_str"],
        dtype={"cid": "int64", "review_str": "string"},
    )

    text = reviews["review_str"].fillna("").str.lower()
    # whitespace tokenizer
    tokens = text.str.split()
    # drop english stop words (mirrors Spark StopWordsRemover default list)
    stop_words = _english_stop_words()
    cleaned = tokens.apply(lambda toks: [t for t in toks if t not in stop_words])
    reviews["positive_review"] = cleaned.apply(lambda toks: "good" in toks)

    log(
        "classified {} reviews ({} positive)".format(
            len(reviews), int(reviews["positive_review"].sum())
        )
    )
    return reviews[["cid", "positive_review"]]


def load_user_purchase() -> pd.DataFrame:
    """Load OnlineRetail.csv using the schema of retail.user_purchase."""
    log(f"reading purchases from {ONLINE_RETAIL_CSV}")
    purchases = pd.read_csv(ONLINE_RETAIL_CSV)
    purchases = purchases.rename(columns=USER_PURCHASE_COLUMNS)
    # CustomerID can be blank for guest checkouts -> keep only identified users
    purchases = purchases.dropna(subset=["customer_id"])
    purchases["customer_id"] = purchases["customer_id"].astype("int64")
    purchases["quantity"] = purchases["quantity"].astype("int64")
    purchases["unit_price"] = purchases["unit_price"].astype("float64")
    return purchases[
        ["customer_id", "quantity", "unit_price"]
    ]


def build_behaviour_metrics(
    purchases: pd.DataFrame, reviews: pd.DataFrame
) -> pd.DataFrame:
    """Mirror create_user_behaviour_metric() SQL from the Airflow DAG."""
    log("joining purchases with reviews and aggregating")
    con = duckdb.connect()
    con.register("up", purchases)
    con.register("mr", reviews)
    query = """
        select
          up.customer_id,
          sum(up.quantity * up.unit_price) as amount_spent,
          cast(sum(case when mr.positive_review then 1 else 0 end) as bigint) as num_positive_reviews,
          count(mr.cid) as num_reviews
        from up
        join mr on up.customer_id = mr.cid
        group by up.customer_id
        order by up.customer_id
    """
    metrics = con.execute(query).fetchdf()
    con.close()
    log(f"computed metrics for {len(metrics)} customers")
    return metrics


def render_dashboard(metrics: pd.DataFrame) -> None:
    """Reproduce dags/scripts/dashboard/dashboard.qmd with Plotly."""
    import plotly.express as px
    import plotly.io as pio

    # average spending per customer (single bar)
    average_spent = pd.DataFrame(
        {"label": ["Average Spending"], "average_spent": [metrics["amount_spent"].mean()]}
    )
    fig_avg = px.bar(
        average_spent,
        x="label",
        y="average_spent",
        title="Average Spending per Customer",
    )

    # spending distribution
    edges = list(range(0, 100000, 10000))  # 0, 10000, ..., 90000
    bins = edges + [float("inf")]  # 11 edges -> 10 bins
    labels = [f"{b}-{b + 9999}" for b in range(0, 90000, 10000)]
    labels.append("90000+")  # 10 labels to match 10 bins
    dist = (
        pd.cut(
            metrics["amount_spent"],
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        .value_counts()
        .reindex(labels)
        .fillna(0)
        .astype(int)
        .rename_axis("spending_bin")
        .reset_index(name="count")
    )
    fig_dist = px.bar(
        dist, x="spending_bin", y="count", title="Spending Distribution"
    )

    fig_avg.update_layout(height=400)
    fig_dist.update_layout(height=400)

    html = (
        "<html><head><meta charset='utf-8'>"
        "<title>CoinCap Exchange Dashboard</title></head><body>"
        "<h1>User Behaviour Metrics</h1>"
        + pio.to_html(fig_avg, full_html=False, include_plotlyjs=True)
        + pio.to_html(fig_dist, full_html=False, include_plotlyjs=False)
        + "</body></html>"
    )
    DASHBOARD_HTML.parent.mkdir(parents=True, exist_ok=True)
    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log(f"wrote dashboard to {DASHBOARD_HTML}")


def _english_stop_words() -> set[str]:
    """Spark's default English stop words list."""
    return {
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
        "your", "yours", "yourself", "yourselves", "he", "him", "his",
        "himself", "she", "her", "hers", "herself", "it", "its", "itself",
        "they", "them", "their", "theirs", "themselves", "what", "which",
        "who", "whom", "this", "that", "these", "those", "am", "is", "are",
        "was", "were", "be", "been", "being", "have", "has", "had", "having",
        "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if",
        "or", "because", "as", "until", "while", "of", "at", "by", "for",
        "with", "about", "against", "between", "into", "through", "during",
        "before", "after", "above", "below", "to", "from", "up", "down",
        "in", "out", "on", "off", "over", "under", "again", "further",
        "then", "once", "here", "there", "when", "where", "why", "how",
        "all", "any", "both", "each", "few", "more", "most", "other", "some",
        "such", "no", "nor", "not", "only", "own", "same", "so", "than",
        "too", "very", "s", "t", "can", "will", "just", "don", "should",
        "now",
    }


def main() -> int:
    if not MOVIE_REVIEW_CSV.exists():
        log(f"missing input: {MOVIE_REVIEW_CSV}")
        return 1
    if not ONLINE_RETAIL_CSV.exists():
        log(f"missing input: {ONLINE_RETAIL_CSV}")
        return 1

    reviews = classify_reviews()
    purchases = load_user_purchase()
    metrics = build_behaviour_metrics(purchases, reviews)
    metrics.to_csv(BEHAVIOUR_METRICS_CSV, index=False)
    log(f"wrote metrics to {BEHAVIOUR_METRICS_CSV}")
    render_dashboard(metrics)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
