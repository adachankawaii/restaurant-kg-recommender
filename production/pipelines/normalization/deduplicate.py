from __future__ import annotations

import pandas as pd


def deduplicate_restaurants(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    aliases = df[["restaurant_id", "name", "normalized_name", "normalized_address", "source_names", "source_record_ids"]].copy()
    canonical = df.drop_duplicates(subset=["restaurant_id"], keep="first").copy()
    return canonical, aliases
