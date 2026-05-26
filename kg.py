import os
import math
import pandas as pd
import numpy as np
import torch
from torch_geometric.data import HeteroData


# =========================
# CONFIG
# =========================
BASE_DIR = "./kg_tables_all"


# =========================
# HELPERS
# =========================
def safe_read_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def normalize_numeric(series):
    series = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if series.max() == series.min():
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - series.mean()) / (series.std() + 1e-8)


def bucket_price(price):
    if pd.isna(price):
        return "unknown"
    price = float(price)
    if price < 30000:
        return "budget"
    elif price < 60000:
        return "mid"
    elif price < 120000:
        return "premium"
    return "luxury"


def parse_time_to_hour(t):
    if pd.isna(t):
        return None
    try:
        hh, mm = str(t).split(":")
        return int(hh) + int(mm) / 60.0
    except Exception:
        return None


def get_open_timeslots(open_time, close_time):
    """
    Gán timeslot đơn giản theo khoảng giờ hoạt động.
    """
    start = parse_time_to_hour(open_time)
    end = parse_time_to_hour(close_time)

    slots = []
    if start is None or end is None:
        return slots

    # quy ước:
    # morning: 5-11
    # afternoon: 11-17
    # evening: 17-22
    # late_night: 22-5
    intervals = {
        "morning": (5, 11),
        "afternoon": (11, 17),
        "evening": (17, 22),
        "late_night": (22, 24),
    }

    for slot, (a, b) in intervals.items():
        if start < b and end > a:
            slots.append(slot)

    if end > 24 or start < 5:
        slots.append("late_night")

    return list(set(slots))


def compute_review_features(review_fact):
    if review_fact.empty:
        return {
            "review_count": 0,
            "avg_rating_5": 0.0,
            "positive_ratio": 0.0,
            "negative_ratio": 0.0,
            "neutral_ratio": 0.0,
            "promo_ratio": 0.0,
            "recent_review_count": 0,
        }

    df = review_fact.copy()

    df["rating_5"] = pd.to_numeric(df["rating_5"], errors="coerce").fillna(0.0)
    review_count = len(df)
    avg_rating_5 = df["rating_5"].mean()

    sentiment_counts = df["sentiment"].fillna("unknown").value_counts()
    positive_ratio = sentiment_counts.get("positive", 0) / review_count
    negative_ratio = sentiment_counts.get("negative", 0) / review_count
    neutral_ratio = sentiment_counts.get("neutral", 0) / review_count

    promo_ratio = pd.to_numeric(df["is_promo"], errors="coerce").fillna(0).mean()

    # recent review count: trong 30 ngày gần nhất so với max rated_at của store
    df["rated_at"] = pd.to_datetime(df["rated_at"], errors="coerce")
    max_time = df["rated_at"].max()
    if pd.isna(max_time):
        recent_review_count = 0
    else:
        recent_review_count = (df["rated_at"] >= (max_time - pd.Timedelta(days=30))).sum()

    return {
        "review_count": review_count,
        "avg_rating_5": float(avg_rating_5),
        "positive_ratio": float(positive_ratio),
        "negative_ratio": float(negative_ratio),
        "neutral_ratio": float(neutral_ratio),
        "promo_ratio": float(promo_ratio),
        "recent_review_count": int(recent_review_count),
    }


# =========================
# MAIN BUILDER
# =========================
def build_hetero_kg(base_dir=BASE_DIR):
    store_dirs = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]

    # mappings
    store2idx = {}
    category2idx = {}
    aspect2idx = {}
    context2idx = {}
    service2idx = {}
    source2idx = {}
    district2idx = {}
    city2idx = {}
    price2idx = {}
    timeslot2idx = {}

    # node features
    store_features = []
    store_meta = []

    # edges
    edge_store_category = [[], []]
    edge_store_aspect = [[], []]
    edge_store_context = [[], []]
    edge_store_service = [[], []]
    edge_store_source = [[], []]
    edge_store_district = [[], []]
    edge_store_city = [[], []]
    edge_store_price = [[], []]
    edge_store_timeslot = [[], []]

    # reverse edges
    rev_edge_category_store = [[], []]
    rev_edge_aspect_store = [[], []]
    rev_edge_context_store = [[], []]
    rev_edge_service_store = [[], []]
    rev_edge_source_store = [[], []]
    rev_edge_district_store = [[], []]
    rev_edge_city_store = [[], []]
    rev_edge_price_store = [[], []]
    rev_edge_timeslot_store = [[], []]

    # optional edge attrs for aspect
    aspect_edge_attr = []

    for store_dir in store_dirs:
        store_path = os.path.join(base_dir, store_dir)

        review_fact = safe_read_csv(os.path.join(store_path, "review_fact.csv"))
        store_aspect_agg = safe_read_csv(os.path.join(store_path, "store_aspect_agg.csv"))
        store_category = safe_read_csv(os.path.join(store_path, "store_category.csv"))
        store_context_tag = safe_read_csv(os.path.join(store_path, "store_context_tag.csv"))
        store_master = safe_read_csv(os.path.join(store_path, "store_master.csv"))
        store_service_option = safe_read_csv(os.path.join(store_path, "store_service_option.csv"))
        store_source_map = safe_read_csv(os.path.join(store_path, "store_source_map.csv"))

        # store_id
        if not store_master.empty and "store_id" in store_master.columns:
            store_id = str(store_master.iloc[0]["store_id"])
        elif not review_fact.empty and "store_id" in review_fact.columns:
            store_id = str(review_fact.iloc[0]["store_id"])
        else:
            store_id = store_dir

        if store_id not in store2idx:
            store2idx[store_id] = len(store2idx)
        s_idx = store2idx[store_id]

        # ===== Store features from store_master + review_fact =====
        review_stats = compute_review_features(review_fact)

        row = store_master.iloc[0] if not store_master.empty else {}

        def get_val(col, default=0.0):
            if isinstance(row, dict):
                return default
            val = row[col] if col in row.index else default
            return 0.0 if pd.isna(val) else val

        feature_dict = {
            "lat": get_val("lat", 0.0),
            "lng": get_val("lng", 0.0),
            "median_price_vnd": get_val("median_price_vnd", 0.0),
            "be_rating_avg": get_val("be_rating_avg", 0.0),
            "be_rating_count": get_val("be_rating_count", 0.0),
            "google_rating": get_val("google_rating", 0.0),
            "google_review_count": get_val("google_review_count", 0.0),
            "review_count": review_stats["review_count"],
            "avg_rating_5": review_stats["avg_rating_5"],
            "positive_ratio": review_stats["positive_ratio"],
            "negative_ratio": review_stats["negative_ratio"],
            "neutral_ratio": review_stats["neutral_ratio"],
            "promo_ratio": review_stats["promo_ratio"],
            "recent_review_count": review_stats["recent_review_count"],
        }

        store_features.append(feature_dict)
        store_meta.append({
            "store_id": store_id,
            "canonical_name": get_val("canonical_name", ""),
            "district": get_val("district", ""),
            "city": get_val("city", ""),
            "status": get_val("status", ""),
        })

        # ===== Category =====
        if not store_category.empty:
            for _, r in store_category.iterrows():
                cat_type = str(r.get("category_type", "unknown"))
                cat_val = str(r.get("category_value", "unknown"))
                node_name = f"{cat_type}::{cat_val}"

                if node_name not in category2idx:
                    category2idx[node_name] = len(category2idx)
                c_idx = category2idx[node_name]

                edge_store_category[0].append(s_idx)
                edge_store_category[1].append(c_idx)
                rev_edge_category_store[0].append(c_idx)
                rev_edge_category_store[1].append(s_idx)

        # ===== Aspect =====
        if not store_aspect_agg.empty:
            for _, r in store_aspect_agg.iterrows():
                aspect = str(r.get("aspect_name", "unknown"))
                if aspect not in aspect2idx:
                    aspect2idx[aspect] = len(aspect2idx)
                a_idx = aspect2idx[aspect]

                edge_store_aspect[0].append(s_idx)
                edge_store_aspect[1].append(a_idx)
                rev_edge_aspect_store[0].append(a_idx)
                rev_edge_aspect_store[1].append(s_idx)

                mention_count = float(pd.to_numeric(pd.Series([r.get("mention_count", 0)]), errors="coerce").fillna(0).iloc[0])
                pos = float(pd.to_numeric(pd.Series([r.get("positive_mentions", 0)]), errors="coerce").fillna(0).iloc[0])
                neg = float(pd.to_numeric(pd.Series([r.get("negative_mentions", 0)]), errors="coerce").fillna(0).iloc[0])
                neu = float(pd.to_numeric(pd.Series([r.get("neutral_mentions", 0)]), errors="coerce").fillna(0).iloc[0])

                aspect_edge_attr.append([mention_count, pos, neg, neu])

        # ===== ContextTag =====
        if not store_context_tag.empty:
            for _, r in store_context_tag.iterrows():
                tag_type = str(r.get("tag_type", "unknown"))
                tag_val = str(r.get("tag_value", "unknown"))
                node_name = f"{tag_type}::{tag_val}"

                if node_name not in context2idx:
                    context2idx[node_name] = len(context2idx)
                ctx_idx = context2idx[node_name]

                edge_store_context[0].append(s_idx)
                edge_store_context[1].append(ctx_idx)
                rev_edge_context_store[0].append(ctx_idx)
                rev_edge_context_store[1].append(s_idx)

        # ===== ServiceOption =====
        if not store_service_option.empty:
            for _, r in store_service_option.iterrows():
                val = pd.to_numeric(pd.Series([r.get("value", 0)]), errors="coerce").fillna(0).iloc[0]
                if int(val) != 1:
                    continue

                service = str(r.get("service_option", "unknown"))
                if service not in service2idx:
                    service2idx[service] = len(service2idx)
                serv_idx = service2idx[service]

                edge_store_service[0].append(s_idx)
                edge_store_service[1].append(serv_idx)
                rev_edge_service_store[0].append(serv_idx)
                rev_edge_service_store[1].append(s_idx)

        # ===== Source =====
        if not store_source_map.empty:
            for _, r in store_source_map.iterrows():
                source = str(r.get("source_name", "unknown"))
                if source not in source2idx:
                    source2idx[source] = len(source2idx)
                src_idx = source2idx[source]

                edge_store_source[0].append(s_idx)
                edge_store_source[1].append(src_idx)
                rev_edge_source_store[0].append(src_idx)
                rev_edge_source_store[1].append(s_idx)

        # ===== District / City =====
        district = str(get_val("district", "")).strip()
        city = str(get_val("city", "")).strip()

        if district:
            if district not in district2idx:
                district2idx[district] = len(district2idx)
            d_idx = district2idx[district]
            edge_store_district[0].append(s_idx)
            edge_store_district[1].append(d_idx)
            rev_edge_district_store[0].append(d_idx)
            rev_edge_district_store[1].append(s_idx)

        if city:
            if city not in city2idx:
                city2idx[city] = len(city2idx)
            c_idx = city2idx[city]
            edge_store_city[0].append(s_idx)
            edge_store_city[1].append(c_idx)
            rev_edge_city_store[0].append(c_idx)
            rev_edge_city_store[1].append(s_idx)

        # ===== PriceRange =====
        price_bucket = bucket_price(get_val("median_price_vnd", np.nan))
        if price_bucket not in price2idx:
            price2idx[price_bucket] = len(price2idx)
        p_idx = price2idx[price_bucket]
        edge_store_price[0].append(s_idx)
        edge_store_price[1].append(p_idx)
        rev_edge_price_store[0].append(p_idx)
        rev_edge_price_store[1].append(s_idx)

        # ===== TimeSlot =====
        open_time = get_val("be_open_time", None)
        close_time = get_val("be_close_time", None)
        timeslots = get_open_timeslots(open_time, close_time)

        for slot in timeslots:
            if slot not in timeslot2idx:
                timeslot2idx[slot] = len(timeslot2idx)
            t_idx = timeslot2idx[slot]
            edge_store_timeslot[0].append(s_idx)
            edge_store_timeslot[1].append(t_idx)
            rev_edge_timeslot_store[0].append(t_idx)
            rev_edge_timeslot_store[1].append(s_idx)

    # =========================
    # Convert store features to tensor
    # =========================
    store_df = pd.DataFrame(store_features).fillna(0.0)

    for col in store_df.columns:
        store_df[col] = pd.to_numeric(store_df[col], errors="coerce").fillna(0.0)

    # normalize numeric features
    store_x = []
    for col in store_df.columns:
        store_x.append(normalize_numeric(store_df[col]).values)

    if len(store_x) > 0:
        store_x = np.stack(store_x, axis=1)
    else:
        store_x = np.zeros((len(store2idx), 1), dtype=np.float32)

    store_x = torch.tensor(store_x, dtype=torch.float)

    # =========================
    # Build HeteroData
    # =========================
    data = HeteroData()

    data["store"].x = store_x
    data["category"].x = torch.ones((len(category2idx), 1), dtype=torch.float)
    data["aspect"].x = torch.ones((len(aspect2idx), 1), dtype=torch.float)
    data["context"].x = torch.ones((len(context2idx), 1), dtype=torch.float)
    data["service"].x = torch.ones((len(service2idx), 1), dtype=torch.float)
    data["source"].x = torch.ones((len(source2idx), 1), dtype=torch.float)
    data["district"].x = torch.ones((len(district2idx), 1), dtype=torch.float)
    data["city"].x = torch.ones((len(city2idx), 1), dtype=torch.float)
    data["price"].x = torch.ones((len(price2idx), 1), dtype=torch.float)
    data["timeslot"].x = torch.ones((len(timeslot2idx), 1), dtype=torch.float)

    def to_edge_index(edge_list):
        if len(edge_list[0]) == 0:
            return torch.empty((2, 0), dtype=torch.long)
        return torch.tensor(edge_list, dtype=torch.long)

    data["store", "has_category", "category"].edge_index = to_edge_index(edge_store_category)
    data["category", "rev_has_category", "store"].edge_index = to_edge_index(rev_edge_category_store)

    data["store", "has_aspect", "aspect"].edge_index = to_edge_index(edge_store_aspect)
    data["aspect", "rev_has_aspect", "store"].edge_index = to_edge_index(rev_edge_aspect_store)

    data["store", "fits_context", "context"].edge_index = to_edge_index(edge_store_context)
    data["context", "rev_fits_context", "store"].edge_index = to_edge_index(rev_edge_context_store)

    data["store", "offers_service", "service"].edge_index = to_edge_index(edge_store_service)
    data["service", "rev_offers_service", "store"].edge_index = to_edge_index(rev_edge_service_store)

    data["store", "listed_on", "source"].edge_index = to_edge_index(edge_store_source)
    data["source", "rev_listed_on", "store"].edge_index = to_edge_index(rev_edge_source_store)

    data["store", "located_in_district", "district"].edge_index = to_edge_index(edge_store_district)
    data["district", "rev_located_in_district", "store"].edge_index = to_edge_index(rev_edge_district_store)

    data["store", "located_in_city", "city"].edge_index = to_edge_index(edge_store_city)
    data["city", "rev_located_in_city", "store"].edge_index = to_edge_index(rev_edge_city_store)

    data["store", "has_price_range", "price"].edge_index = to_edge_index(edge_store_price)
    data["price", "rev_has_price_range", "store"].edge_index = to_edge_index(rev_edge_price_store)

    data["store", "open_in_timeslot", "timeslot"].edge_index = to_edge_index(edge_store_timeslot)
    data["timeslot", "rev_open_in_timeslot", "store"].edge_index = to_edge_index(rev_edge_timeslot_store)

    # edge attributes for aspect relation
    if len(aspect_edge_attr) > 0:
        data["store", "has_aspect", "aspect"].edge_attr = torch.tensor(aspect_edge_attr, dtype=torch.float)

    return data, store_df, store_meta


import networkx as nx
import matplotlib.pyplot as plt

def hetero_to_networkx(data):
    G = nx.Graph()

    # Add nodes
    for node_type in data.node_types:
        num_nodes = data[node_type].num_nodes
        for i in range(num_nodes):
            G.add_node(f"{node_type}_{i}", type=node_type)

    # Add edges
    for (src, rel, dst) in data.edge_types:
        edge_index = data[(src, rel, dst)].edge_index
        for i in range(edge_index.shape[1]):
            u = edge_index[0, i].item()
            v = edge_index[1, i].item()
            G.add_edge(f"{src}_{u}", f"{dst}_{v}", label=rel)

    return G
    
def visualize_store_subgraph(G, store_idx=0):
    store_node = f"store_{store_idx}"

    # Lấy neighbors
    neighbors = list(G.neighbors(store_node))
    sub_nodes = [store_node] + neighbors

    subG = G.subgraph(sub_nodes)

    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(subG, seed=42)

    # Màu theo loại node
    color_map = []
    for node in subG.nodes:
        if node.startswith("store"):
            color_map.append("red")
        elif node.startswith("category"):
            color_map.append("blue")
        elif node.startswith("aspect"):
            color_map.append("green")
        elif node.startswith("context"):
            color_map.append("orange")
        elif node.startswith("service"):
            color_map.append("purple")
        else:
            color_map.append("gray")

    nx.draw(
        subG, pos,
        node_color=color_map,
        with_labels=True,
        node_size=1200,
        font_size=8
    )

    edge_labels = nx.get_edge_attributes(subG, "label")
    nx.draw_networkx_edge_labels(subG, pos, edge_labels=edge_labels, font_size=7)

    plt.title(f"Subgraph của Store {store_idx}")
    plt.show()

if __name__ == "__main__":
    data, store_df, store_meta = build_hetero_kg(BASE_DIR)

    G = hetero_to_networkx(data)

    visualize_store_subgraph(G, store_idx=0)