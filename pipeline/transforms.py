"""Business transforms: review classification, behaviour metrics, dashboard.

The logic is ported from the original ``pipeline/run_local.py`` (which in turn
mirrored the Airflow + Spark scripts) so the business meaning is unchanged:

* a review is *positive* when its cleaned tokens contain the word ``good``
* behaviour metrics join purchases with reviews per customer
* the dashboard renders average spending and a spending distribution
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# review classification (mirrors dags/scripts/spark/random_text_classification)
# ---------------------------------------------------------------------------

#: Spark's default English stop word list.
ENGLISH_STOP_WORDS: frozenset[str] = frozenset(
    {
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
)


def classify_review(text: str | None) -> bool:
    """Return True when the cleaned review tokens contain ``good``."""
    if text is None:
        return False
    tokens = str(text).lower().split()
    return "good" in [token for token in tokens if token not in ENGLISH_STOP_WORDS]


# ---------------------------------------------------------------------------
# behaviour metrics
# ---------------------------------------------------------------------------

_METRIC_COLUMNS = (
    "customer_id",
    "amount_spent",
    "num_positive_reviews",
    "num_reviews",
)


def build_metrics(connection: duckdb.DuckDBPyConnection) -> int:
    """Recompute ``customer_behaviour_metrics`` from the raw tables.

    Full recomputation keeps the table correct even when a batch is replayed;
    the delete + insert runs inside one transaction so readers never see a
    half-built table.
    """
    connection.begin()
    try:
        connection.execute("delete from customer_behaviour_metrics")
        connection.execute(
            """
            insert into customer_behaviour_metrics
                (customer_id, amount_spent, num_positive_reviews,
                 num_reviews, updated_at)
            select
                up.customer_id,
                sum(up.quantity * up.unit_price) as amount_spent,
                cast(sum(case when mr.positive_review then 1 else 0 end) as bigint)
                    as num_positive_reviews,
                count(mr.source_row_id) as num_reviews,
                now()
            from raw_user_purchase up
            join raw_movie_review mr on up.customer_id = mr.cid
            group by up.customer_id
            """
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return connection.execute(
        "select count(*) from customer_behaviour_metrics"
    ).fetchone()[0]


def read_metrics(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return the current behaviour metrics as a DataFrame."""
    return connection.execute(
        f"select {', '.join(_METRIC_COLUMNS)} "
        "from customer_behaviour_metrics order by customer_id"
    ).fetchdf()


def write_metrics_csv(path: str | Path, metrics: pd.DataFrame) -> None:
    """Write the metrics table to CSV."""
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(resolved, index=False)


# ---------------------------------------------------------------------------
# dashboard (mirrors dags/scripts/dashboard/dashboard.qmd)
# ---------------------------------------------------------------------------


def render_dashboard(path: str | Path, metrics: pd.DataFrame) -> None:
    """Render the Plotly dashboard to a standalone HTML file."""
    import plotly.express as px
    import plotly.io as pio

    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if metrics.empty:
        average_spent = 0.0
    else:
        average_spent = float(metrics["amount_spent"].mean())

    fig_avg = px.bar(
        pd.DataFrame({"label": ["Average Spending"], "average_spent": [average_spent]}),
        x="label",
        y="average_spent",
        title="Average Spending per Customer",
    )

    edges = list(range(0, 100000, 10000))
    bins = edges + [float("inf")]
    labels = [f"{b}-{b + 9999}" for b in range(0, 90000, 10000)]
    labels.append("90000+")
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
        "<title>User Behaviour Dashboard</title></head><body>"
        "<h1>User Behaviour Metrics</h1>"
        + pio.to_html(fig_avg, full_html=False, include_plotlyjs=True)
        + pio.to_html(fig_dist, full_html=False, include_plotlyjs=False)
        + "</body></html>"
    )
    resolved.write_text(html, encoding="utf-8")