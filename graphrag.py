# -*- coding: utf-8 -*-

import os
import re
import ast
import json
import glob
import logging
import math
from collections import defaultdict
from operator import itemgetter
from pathlib import Path

import numpy as np
import pandas as pd
import networkx as nx
from openai import OpenAI
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain.docstore.document import Document

# NOTE:
# - Removed llama_index SemanticChunker + extra loaders. We now retrieve from `store_docs.jsonl` built by kg_creation.py.
# - This script is adapted to the food KG in this repo.

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
api_key = os.getenv("OPENAI_API_KEY")

# ------------------------------
# Prompts / Config
# ------------------------------
# This repo does not ship `system_prompts/` anymore.
# Keep prompts inline so the script runs out-of-the-box.

SYSTEM_PROMPT_FACT_EXTRACT = """Bạn là bộ phân tích truy vấn cho hệ gợi ý quán ăn.
Trả về một Python list literal (để ast.literal_eval parse được) theo format:
[
  <query_type>,
  <slot_1>, <slot_2>, ...
]
Trong đó query_type ∈ {RECOMMEND, FILTER, DETAIL, CHITCHAT}.
Slot có thể là: món ăn, category, service (delivery/takeaway), context (date/family), khu vực, budget.
Nếu không rõ thì dùng CHITCHAT.
"""

SYSTEM_PROMPT_ANSWER = """Bạn là trợ lý gợi ý quán ăn dựa trên Knowledge Graph và review.
Luôn trả lời tiếng Việt, ngắn gọn, có lý do (explain) dựa trên các evidence được cung cấp.
Nếu thiếu dữ liệu thì nói rõ thiếu gì.
"""

# ------------------------------
# Load KG graph data produced by `kg_creation.py`
# ------------------------------
# nodes/edges are deterministic and can be imported to Neo4j.
# This script uses them for lightweight Graph-RAG.

KG_GRAPH_DIR = Path("kg_tables_all") / "kg_graph"
NODES_CSV = KG_GRAPH_DIR / "nodes.csv"
EDGES_CSV = KG_GRAPH_DIR / "edges.csv"
STORE_DOCS_JSONL = KG_GRAPH_DIR / "store_docs.jsonl"

# Load graph edges into a DataFrame
# columns: source_id, relation, target_id, properties
if not EDGES_CSV.exists():
    raise FileNotFoundError(f"Missing {EDGES_CSV}. Run: python kg_creation.py")

df_graph = pd.read_csv(EDGES_CSV)

# ------------------------------
# Build an in-memory NetworkX graph from edges.csv
# ------------------------------

def build_nx_graph_from_edges(df_edges: pd.DataFrame) -> nx.Graph:
    """Create an undirected graph for traversal / shortest-path heuristics.

    We store relation + properties_json on the edge for later explanation.
    """

    G = nx.Graph()
    for _, row in df_edges.iterrows():
        s = str(row.get("source_id", ""))
        t = str(row.get("target_id", ""))
        if not s or not t:
            continue
        rel = str(row.get("relation", ""))
        props = row.get("properties", "{}")

        # A simple default weight; you can refine (e.g., review edges weight by rating)
        weight = 1.0
        if rel == "REVIEWS":
            try:
                props_obj = json.loads(props) if isinstance(props, str) else {}
                rating = float(props_obj.get("rating_5")) if props_obj.get("rating_5") is not None else None
                if rating is not None:
                    weight = max(0.1, 6.0 - rating)  # higher rating => smaller distance
            except Exception:
                pass

        G.add_edge(s, t, weight=weight, title=rel, properties=props)

    return G


def load_store_docs(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run: python kg_creation.py")
    docs: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            docs.append(json.loads(line))
    return docs

# function to get top-n contextually closest nodes and edges for a given node
def get_top_contextual_nodes_edges(graph, node, top_n=5):
    if node not in graph:
        return []
    neighbors = [(n, graph[node][n]['weight']) for n in graph.neighbors(node)]
    sorted_neighbors = sorted(neighbors, key=itemgetter(1), reverse=True)
    return sorted_neighbors[:top_n]

# Build the graph from kg_tables_all/kg_graph/edges.csv
graph = build_nx_graph_from_edges(df_graph)

# Load store documents (Graph-RAG corpus)
store_docs = load_store_docs(STORE_DOCS_JSONL)

# ==============================
# Calculate tf-idf-scores for all nodes in context
# ==============================
#region tf-idf logic for nodes context

#Calculate Term Frequency (TF) for nodes context
# Initialize a dictionary to count occurrences
node_context_count = {}
total_contexts = set()

# Iterate over the edges in the NetworkX graph
for (source, target, data) in graph.edges(data=True):
    context = data.get('title', 'No title')
    total_contexts.add(context)

    if source not in node_context_count:
        node_context_count[source] = {}
    if target not in node_context_count:
        node_context_count[target] = {}

    node_context_count[source].setdefault(context, 0)
    node_context_count[target].setdefault(context, 0)
    node_context_count[source][context] += 1
    node_context_count[target][context] += 1

# Calculate Inverse Document Frequency (IDF)
idf_scores = {}
num_contexts = len(total_contexts)

for node, contexts in node_context_count.items():
    idf_scores[node] = math.log(num_contexts / len(contexts))

# Calculate TF-IDF Scores
tf_idf_scores = {}

for node, contexts in node_context_count.items():
    tf_idf_scores[node] = {}
    for context, count in contexts.items():
        tf = count / len(contexts)
        idf = idf_scores[node]
        tf_idf_scores[node][context] = tf * idf
# for custom algo:
# Extract a single TF-IDF score per node (e.g., the maximum score)
single_tf_idf_scores = {node: max(contexts.values()) for node, contexts in tf_idf_scores.items()}

# endregion

# ------------------------------
# Embeddings + similarity over edges.csv schema
# ------------------------------

def _edge_text(row: pd.Series) -> str:
    """Create a text representation for an edge suitable for embedding."""

    s = str(row.get("source_id", ""))
    r = str(row.get("relation", ""))
    t = str(row.get("target_id", ""))
    props = row.get("properties", "")
    if props is None:
        props = ""
    props = str(props)
    # Keep properties short-ish (some rows might have long json)
    if len(props) > 500:
        props = props[:500] + "…"
    return f"{s} --[{r}]--> {t} | {props}"


def generate_embeddings_list(texts: list[str]) -> list[list[float]]:
    openai_api = OpenAIEmbeddings()
    return openai_api.embed_documents([str(t) for t in texts])


# Build edge_text + embeddings once
if "edge_text" not in df_graph.columns:
    df_graph["edge_text"] = df_graph.apply(_edge_text, axis=1)

df_graph["edge_embedding"] = generate_embeddings_list(df_graph["edge_text"].astype(str).tolist())

# ==============================
# topic clustering of nodes (based on averaged incident edge embeddings)
# ==============================

node_embeddings_sum = defaultdict(list)
for _, row in df_graph.iterrows():
    e = row.get("edge_embedding")
    if e is None:
        continue
    s = str(row.get("source_id", ""))
    t = str(row.get("target_id", ""))
    if s:
        node_embeddings_sum[s].append(e)
    if t:
        node_embeddings_sum[t].append(e)

node_embeddings_avg = {node: np.mean(embs, axis=0) for node, embs in node_embeddings_sum.items() if len(embs) > 0}

if node_embeddings_avg:
    nodes, embeddings = zip(*node_embeddings_avg.items())
    embeddings_list = list(embeddings)

    n_clusters = 5
    kmeans = KMeans(n_clusters=n_clusters)
    clusters = kmeans.fit_predict(embeddings_list)
    node_cluster_mapping = dict(zip(nodes, clusters))
else:
    node_cluster_mapping = {}


def find_similar_nodes_and_edges(query: str, df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Return top-N most similar edges to the query.

    Output columns: source_id, relation, target_id, similarity, edge_text
    """

    print("start find_similar_nodes_and_edges")
    query_embedding = OpenAIEmbeddings().embed_query(query)

    def sim(row: pd.Series) -> float:
        emb = row.get("edge_embedding")
        if emb is None:
            return 0.0
        # cosine_similarity expects 2D arrays
        return float(cosine_similarity([emb], [query_embedding])[0][0])

    df = df.copy()
    df["similarity"] = df.apply(sim, axis=1)
    top = df.nlargest(top_n, "similarity")
    return top[["source_id", "relation", "target_id", "similarity", "edge_text"]]

# function to get text_chunks from chunk Id based on similar nodes with cosine similarity
def get_text_chunks_store_docs(store_ids, store_docs, k_per_store: int = 1):
    """Return short evidence strings for a list of store_ids from store_docs.jsonl."""

    by_store = {str(d.get("store_id")): d for d in store_docs}
    chunks = []
    for sid in store_ids:
        doc = by_store.get(str(sid))
        if not doc:
            continue
        text = str(doc.get("text", "")).strip()
        if not text:
            continue
        # Keep it short to feed into the final LLM call
        if len(text) > 1200:
            text = text[:1200] + "…"
        chunks.append(text)
    return chunks

# function to extract cosine scores for vector searched nodes based on query
def extract_cosine_scores(similar_edges_df: pd.DataFrame) -> dict[str, float]:
    """Aggregate similarity scores per node from a similar-edges dataframe."""

    cosine_scores: dict[str, float] = {}
    for _, row in similar_edges_df.iterrows():
        nodes = [row.get('source_id'), row.get('target_id')]
        for node in nodes:
            if not isinstance(node, str) or not node:
                continue
            sim = float(row.get('similarity', 0.0) or 0.0)
            cosine_scores[node] = max(sim, cosine_scores.get(node, 0.0))
    return cosine_scores

# function to apply Dijkstra on all node pairs from vector searched nodes
def find_all_shortest_paths(graph, nodes):
    all_paths_str = []

    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            try:
                path = nx.shortest_path(graph, source=nodes[i], target=nodes[j], weight='weight')
                path_str = construct_path_string(path, graph)
                all_paths_str.append(path_str)
            except nx.NetworkXNoPath:
                continue  # If there's no path between a pair of nodes, just skip

    return all_paths_str

# function to make path_string for Dijkstra-paths
def construct_path_string(path, graph):
    path_str = ''
    for i in range(len(path)):
        node = path[i]
        if i > 0:
            prev_node = path[i - 1]
            edge_data = graph.get_edge_data(prev_node, node)
            edge_description = edge_data.get('title', 'Unnamed Edge')
            path_str += f" - {edge_description} - "
        path_str += f"{node}"
    return path_str

# function to apply traveling salesman problem (variation, no circle) on vector-searched nodes
def find_tsp_path(graph, start_node, nodes):
    path = [start_node]
    current_node = start_node
    visited = set([start_node])

    while len(visited) < len(nodes):
        neighbors = [(n, graph[current_node][n]['weight']) for n in graph.neighbors(current_node) if n not in visited]
        if not neighbors:
            break  # No unvisited neighbors, break the loop

        # Sort neighbors by weight
        neighbors.sort(key=lambda x: x[1])

        # Choose the nearest neighbor
        next_node = neighbors[0][0]
        path.append(next_node)
        visited.add(next_node)
        current_node = next_node

    return path

# function to get context_triplets from topic clustering, top n results based on weight
def get_context_triplets(graph, context_nodes, top_n):
    context_triplets = []

    for u, v, data in graph.edges(data=True):
        if u in context_nodes and v in context_nodes:
            edge_description = data.get('title', 'Unnamed Edge')
            edge_weight = data.get('weight', 0)
            triplet = (u, edge_description, v, edge_weight)
            context_triplets.append(triplet)

    # Sort the triplets based on weight and get the top-n results
    context_triplets.sort(key=lambda x: x[3], reverse=True)  # Sorting by edge weight
    return context_triplets[:top_n]

# function to get context_triplets from topic clustering, no weights, all of them
def get_context_triplets_no_weights(graph, context_nodes):
    context_triplets = []

    for u, v, data in graph.edges(data=True):
        if u in context_nodes and v in context_nodes:
            edge_description = data.get('title', 'Unnamed Edge')
            triplet = (u, edge_description, v)
            context_triplets.append(triplet)

    return context_triplets

# function for similarity_search for text
def find_similar_store_docs(query, vectorstore, k=5):
    """Retrieve similar store documents from Chroma.

    Returns list[str] of page_content.
    """

    results = vectorstore.similarity_search_with_score(query, k=k)
    return [doc.page_content for doc, _score in results]

# function to apply custom algo to find top_nodes
def generate_context(graph, df_graph, query, tf_idf_scores, top_n=5):
    # Initialize the scaler
    scaler = MinMaxScaler()
    combined_scores = {}  # Initialize the dictionary

    #   Centrality Measures: Highlights structurally significant nodes in the graph with PageRank

    # Calculate centrality measures in the graph
    centrality = nx.pagerank(graph)  # or other centrality measures
    # Normalize the centrality scores
    centrality_values = list(centrality.values())
    centrality_normalized = scaler.fit_transform(np.array(centrality_values).reshape(-1, 1)).flatten()

    # Map back the normalized scores to nodes
    normalized_centrality = dict(zip(centrality.keys(), centrality_normalized))

    #   TF-IDF Scores: Signifies the uniqueness and importance of nodes in various contexts

    tf_idf_values = list(single_tf_idf_scores.values())
    tf_idf_normalized = scaler.fit_transform(np.array(tf_idf_values).reshape(-1, 1)).flatten()

    # Map back the normalized scores to nodes
    normalized_tf_idf_scores = dict(zip(single_tf_idf_scores.keys(), tf_idf_normalized))

    #    Cosine Similarity: Determines the relevance of nodes to the query

    # Find similar nodes and edges
    similar_nodes_df = find_similar_nodes_and_edges(query, df_graph, top_n=10)

    # Extract cosine similarity scores for individual nodes
    cosine_scores = extract_cosine_scores(similar_nodes_df)

    #   combined scores for top nodes
    for node in graph.nodes():
        cosine_score = cosine_scores.get(node, 0)
        tf_idf_score = normalized_tf_idf_scores.get(node, 0)
        centrality_score = normalized_centrality.get(node, 0)
        combined_scores[node] = cosine_score + tf_idf_score + centrality_score

    # Select top scoring nodes
    top_nodes = sorted(combined_scores, key=combined_scores.get, reverse=True)[:top_n]
    return top_nodes

# function to get text chunks for top nodes
def get_text_chunks_for_top_nodes(top_nodes, df_graph, chunks_dataframe):
    """Legacy helper kept for compatibility.

    In the new pipeline we no longer rely on chunk_id; we just map store:<id> -> store_docs.
    """

    store_ids = [n.split(":", 1)[1] for n in top_nodes if isinstance(n, str) and n.startswith("store:")]
    return get_text_chunks_store_docs(store_ids, store_docs)

# function to Generate Response with OpenAI API for fact extract
def generate_fact_extract(query):

    print("start fact extract")
    client = OpenAI(api_key=api_key)

    # Use inline prompt (no system_prompts folder)
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT_FACT_EXTRACT},
            {"role": "user", "content": query},
        ],
    )

    response = completion.choices[0].message.content #only the content
    print("Got response from LLM: ")
    # Convert the string representation of a list to an actual list
    response_list = ast.literal_eval(response)

    return response_list

#function to determine the system_prompt based on the first entry in the list returned from fact_extract
def get_system_prompt(fact_extract):
    # Single generic prompt for food recommender.
    # (You can branch by fact_extract[0] later if you want.)
    return SYSTEM_PROMPT_ANSWER

# function to Generate Response with OpenAI API
def generate_response(query, fact_extract):

    # ==============================
    # find similar_nodes with cos similarity
    # ==============================

    # Similar edges (cosine similarity over embedded edge_text)
    similar_edges = [find_similar_nodes_and_edges(str(q), df_graph, top_n=3) for q in fact_extract]

    graph_info = "\n".join(
        [f"{row['source_id']} --[{row['relation']}]--> {row['target_id']} (sim={row['similarity']:.3f})" 
        for df in similar_edges for _, row in df.iterrows()]
    )

    # Fetch store evidence if any endpoint is a store:<id>
    store_ids_from_similar: list[str] = []
    for df in similar_edges:
        for _, row in df.iterrows():
            for n in [row.get('source_id'), row.get('target_id')]:
                if isinstance(n, str) and n.startswith('store:'):
                    store_ids_from_similar.append(n.split(':', 1)[1])

    cosine_nodes_chunks = get_text_chunks_store_docs(store_ids_from_similar, store_docs)
    cosine_nodes_chunks_str = '\n'.join(cosine_nodes_chunks)

    # ==============================
    # TSP / contextual neighborhoods / clustering based on similar edge endpoints
    # ==============================

    # Build a list of candidate nodes from the similar edges' endpoints
    similar_nodes_list = []
    for df in similar_edges:
        for _, row in df.iterrows():
            for n in [row.get("source_id"), row.get("target_id")]:
                if isinstance(n, str) and n:
                    similar_nodes_list.append(n)

    # Deduplicate but keep order
    seen = set()
    similar_nodes_list = [n for n in similar_nodes_list if not (n in seen or seen.add(n))]

    # TSP path (heuristic) across these nodes (only if >= 2 nodes)
    tsp_path_str = ""
    if len(similar_nodes_list) >= 2:
        tsp_path = find_tsp_path(graph, similar_nodes_list[0], similar_nodes_list)
        for i, node in enumerate(tsp_path):
            if i > 0:
                prev_node = tsp_path[i - 1]
                edge_data = graph.get_edge_data(prev_node, node) or {}
                edge_description = edge_data.get('title', 'REL')
                tsp_path_str += f" - {edge_description} - "
            tsp_path_str += f"{node}"

    # shortest paths between all pairs (optional info)
    shortest_paths_strings = find_all_shortest_paths(graph, similar_nodes_list) if len(similar_nodes_list) >= 2 else []

    # Contextual neighbors for each node in similar_nodes_list
    contextual_nodes_info = ""
    for node in similar_nodes_list[:10]:
        top_nodes_edges = get_top_contextual_nodes_edges(graph, node, top_n=2)
        contextual_nodes_info += f"Node: {node}\n"
        for neighbor, weight in top_nodes_edges:
            tf_idf_score = tf_idf_scores.get(node, {}).get(neighbor, 0)
            combined_score = 0.5 * tf_idf_score + 0.4 * weight
            edge_data = graph.get_edge_data(node, neighbor) or {}
            edge_title = edge_data.get('title', '')
            contextual_nodes_info += f" - Neighbor: {neighbor}, Edge: {edge_title}, Combined Score: {combined_score}\n"

    # ==============================
    # topic clustering of nodes
    # ==============================

    # Find the clusters of the similar nodes
    similar_nodes_clusters = {node_cluster_mapping[node] for node in similar_nodes_list if node in node_cluster_mapping}

    # Get all nodes from these clusters
    context_nodes = [node for node, cluster in node_cluster_mapping.items() if cluster in similar_nodes_clusters]
    #print("topic clusters: ", context_nodes)

    # Get the top-n topic clustered triplets (weighted)
    context_triplets = get_context_triplets(graph, context_nodes, top_n=3)
    # or context_triplets without the weights
    context_triplets_all = get_context_triplets_no_weights(graph, context_nodes)

    # Format these triplets as a string
    topic_triplets_str = "\n".join([f"{u} - {edge} - {v}" for u, edge, v, _ in context_triplets])
    #print("context triplets_with_weights: ", triplets_str)

    # ==============================
    # custom algo for top nodes
    # ==============================

    # Generate context using the custom algorithm
    top_nodes = [generate_context(graph, df_graph, query, tf_idf_scores, top_n=1) for query in fact_extract]
    # Flattening the list of lists
    top_nodes = [node for sublist in top_nodes for node in sublist]

    # Ensure top_nodes exist in the NetworkX graph
    top_nodes = [n for n in top_nodes if n in graph]
    print("top_nodes: ", top_nodes)

    # Extract node-edge-node triplets for these top nodes
    top_nodes_triplets = get_context_triplets(graph, top_nodes, top_n = 2)
    #print("top nodes_triplets: ", top_nodes_triplets)
    top_nodes_triplets_str = "\n".join([f"{u} - {edge} - {v}" for u, edge, v, _ in top_nodes_triplets])
    print("top nodes_triplets: ", top_nodes_triplets)

    # get top chunks based on top_nodes chunks_id
    #top_nodes_chunks = get_text_chunks_for_top_nodes(top_nodes, df_graph, chunks_dataframe)

    # Formatting the top nodes chunks for display
    #top_nodes_chunks_str = '\n'.join(top_nodes_chunks)

    # ==============================
    # get contextual_ nodes with tf-idf balanced with weights for top_nodes
    # ==============================

    # Initialize a string to hold contextual node information
    contextual_nodes_info_top_nodes = ""

    # tf-idf neighbors with balanced weight between tf-idf-score and weight
    for node in top_nodes:
        # Retrieve the top neighbors based on edge weight
        top_nodes_edges = get_top_contextual_nodes_edges(graph, node, top_n=2)

        contextual_nodes_info_top_nodes += f"Node: {node}\n"
        for neighbor, weight in top_nodes_edges:
            tf_idf_score = tf_idf_scores.get(node, {}).get(neighbor, 0)
            # Combine the scores: Adjust the '0.5' factors to tweak the balance
            combined_score = 0.5 * tf_idf_score + 0.4 * weight

            edge_data = graph.get_edge_data(node, neighbor) or {}
            edge_title = edge_data.get('title', '')
            contextual_info = f" - Contextual Neighbor: {neighbor}, Edge: {edge_title}, Combined Score: {combined_score}\n"
            contextual_nodes_info_top_nodes += contextual_info

    #print("top nodes contextual info with tfidf- balanced with weight: ", contextual_nodes_info_top_nodes)

    # ==============================
    # get text chunks with cos similarity
    # ==============================

    # Retrieve similar store docs (Graph-RAG)
    similar_docs_for_query = find_similar_store_docs(query, text_vectorstore, k=5)
    similar_docs_for_facts = [find_similar_store_docs(q, text_vectorstore, k=3) for q in fact_extract]
    print("Apply similarity search for store_docs")

    references_query = "\n\n".join(similar_docs_for_query)
    references = "\n\n".join(["\n\n".join(docs) for docs in similar_docs_for_facts])

    # ==============================
    # combine all context
    # ==============================

    #toDO: for KG Contexts - use the triplets str or just topic clustered nodes for example?
    # Combine context from KG, references, and contextual nodes
    combined_context = (
                        f"Câu hỏi người dùng:\n{query}"
                        f"\n\nFact extract (machine-readable):\n{fact_extract}"
                        f"\n\nNgữ cảnh tổng hợp từ Knowledge Graph (A) và tài liệu tham chiếu (B):\n"
                        f"A. Ngữ cảnh từ Knowledge Graph\n"
                        f"1) Các node/edge gần nhất theo cosine similarity (KG vector search):\n{graph_info}\n\n"
                        f"2) Hàng xóm ngữ cảnh của các node ở (1):\n{contextual_nodes_info}\n\n"
                        f"3) Đường đi nối các node đã tìm (heuristic TSP):\n{tsp_path_str}\n\n"
                        f"4) Cụm chủ đề (topic clusters) liên quan truy vấn:\n{topic_triplets_str}\n\n"
                        f"5) Top-nodes theo thuật toán kết hợp (centrality + tf-idf + cosine):\n{top_nodes_triplets_str}\n\n"
                        f"6) Hàng xóm ngữ cảnh của các top-nodes (5):\n{contextual_nodes_info_top_nodes}\n\n"
                        f"B. Ngữ cảnh từ corpus (store_docs)\n{references_query}\n\n{references}\n"
                        )
    #print(combined_context)

    # ==============================
    # make LLM call
    # ==============================

    system_prompt = get_system_prompt(fact_extract)

    print("start LLM call")
    client = OpenAI(api_key=api_key)
    #gpt-4-1106-preview #gpt-3.5-turbo-1106
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=[
            {"role": "system",
             "content":
                 f"{system_prompt}"
             },
            {"role": "user", "content": combined_context}
        ]
    )

    # Generate response using combined context
    #response = completion.choices[0].message
    response = completion.choices[0].message.content #only the content
    print("Got response from LLM: ")
    # Combine context and response
    full_interaction = (
                        f"\n\nCâu hỏi người dùng:\n{query}"
                        f"\n\nFact extract (machine-readable):\n{fact_extract}"
                        f"\n\nTrả lời của LLM:\n{response}"
                        f"\n\nNgữ cảnh tổng hợp từ Knowledge Graph (A) và tài liệu tham chiếu (B):\n"
                        f"A. Ngữ cảnh từ Knowledge Graph\n"
                        f"1) Các node/edge gần nhất theo cosine similarity (KG vector search):\n{graph_info}\n\n"
                        f"2) Hàng xóm ngữ cảnh của các node ở (1):\n{contextual_nodes_info}\n\n"
                        f"3) Đường đi nối các node đã tìm (heuristic TSP):\n{tsp_path_str}\n\n"
                        f"4) Cụm chủ đề (topic clusters) liên quan truy vấn:\n{topic_triplets_str}\n\n"
                        f"5) Top-nodes theo thuật toán kết hợp (centrality + tf-idf + cosine):\n{top_nodes_triplets_str}\n\n"
                        f"6) Hàng xóm ngữ cảnh của các top-nodes (5):\n{contextual_nodes_info_top_nodes}\n\n"
                        f"B. Ngữ cảnh từ corpus (store_docs)\n{references_query}\n\n{references}\n"
                        )
    return full_interaction

# ==============================
# Vectorstores (Graph-RAG)
# ==============================
# We build/consume vectorstores from `kg_tables_all/kg_graph/store_docs.jsonl`.
# This avoids the old data_input/*.txt corpus pipeline.

text_vectorstore_path = "vectorstores/store_docs"

if os.path.exists(text_vectorstore_path):
    print("Loading existing vectorstore for store_docs")
    text_vectorstore = Chroma(persist_directory=text_vectorstore_path, embedding_function=OpenAIEmbeddings())
else:
    print("Creating vectorstore for store_docs")
    docs = [Document(page_content=d["text"], metadata=d.get("metadata", {})) for d in store_docs]
    text_vectorstore = Chroma.from_documents(docs, embedding=OpenAIEmbeddings(), persist_directory=text_vectorstore_path)

# Optional: vectorstore for KG strings (relation types / node ids) - keep simple
kg_vectorstore_path = "vectorstores/kg_strings"

if os.path.exists(kg_vectorstore_path):
    print("Loading existing vectorstore for KG strings")
    kg_vectorstore = Chroma(persist_directory=kg_vectorstore_path, embedding_function=OpenAIEmbeddings())
else:
    print("Creating vectorstore for KG strings")
    kg_texts = (
        df_graph["source_id"].astype(str).tolist()
        + df_graph["target_id"].astype(str).tolist()
        + df_graph["relation"].astype(str).tolist()
    )
    kg_vectorstore = Chroma.from_texts(kg_texts, OpenAIEmbeddings(), persist_directory=kg_vectorstore_path)

# Main Loop for User Interaction
chat_history = []
query = None

while True:
    query = input("Prompt: ")
    if query.lower() in ['quit', 'q', 'exit']:
        break
    
    fact_extract = generate_fact_extract(query)
    print(fact_extract)
    full_interaction = generate_response(query, fact_extract)
    print(full_interaction)
    chat_history.append(full_interaction)

# At the end of your chat session
filename = './data_output/history/chat_history.txt'
with open(filename, 'a') as file:
    file.write("\n\n".join(chat_history) + "\n\n")