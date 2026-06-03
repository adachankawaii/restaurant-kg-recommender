from __future__ import annotations

from rgcn_pipeline.phase2_finetune import slugify
from rgcn_pipeline.src.data_loader import (
    _graphrag_node_id,
    _graphrag_node_type,
    _slugify_id,
    summarize_graph_csvs,
)


def test_graphrag_menu_nodes_are_supported_for_rgcn_export():
    assert _graphrag_node_id(["MenuItem"], {"menu_item_id": "123"}) == "menu_item:123"
    assert _graphrag_node_id(["MenuCategory"], {"name": "Mon chay"}) == "menu_category:mon_chay"
    assert _graphrag_node_type(["MenuItem"]) == "MenuItem"
    assert _graphrag_node_type(["MenuCategory"]) == "MenuCategory"


def test_graphrag_core_nodes_are_supported_for_rgcn_export():
    assert _graphrag_node_id(["Restaurant"], {"store_key": "42"}) == "store:42"
    assert _graphrag_node_id(["TextUnit"], {"text_unit_id": "tu-1"}) == "text_unit:tu-1"
    assert _graphrag_node_id(["Attribute"], {"store_key": "42", "type": "food_quality"}) == "attribute:42:food_quality"
    assert _graphrag_node_id(["Area"], {"area_id": "Ha Noi:Quan Dong Da"}) == "area:ha_noi_quan_dong_da"
    assert _graphrag_node_id(["DishEntity"], {"name": "Com ga"}) == "dish:com_ga"
    assert _graphrag_node_id(["DishFamily"], {"name": "Com ga"}) == "dish_family:com_ga"
    assert (
        _graphrag_node_id(["ExtractedEntity"], {"entity_key": "dish:com_ga", "name": "Com ga"})
        == "extracted_entity:dish:com_ga"
    )
    assert (
        _graphrag_node_id(["ExtractedRelation"], {"relation_key": "abc123"})
        == "extracted_relation:abc123"
    )
    assert _graphrag_node_id(["Community"], {"community_id": "7"}) == "community:7"
    assert _graphrag_node_id(["CommunityReport"], {"report_id": "community_report_7"}) == "community_report:community_report_7"
    assert _graphrag_node_type(["Restaurant"]) == "Store"
    assert _graphrag_node_type(["ExtractedEntity"]) == "ExtractedEntity"


def test_vietnamese_ids_match_between_export_and_query_features():
    text = "\u0110\u1ed3 \u0103n chay"
    assert _slugify_id(text) == "do_an_chay"
    assert slugify(text) == "do_an_chay"


def test_summarize_graph_csvs_counts_graphrag_communities(tmp_path):
    nodes_path = tmp_path / "nodes.csv"
    edges_path = tmp_path / "edges.csv"
    nodes_path.write_text(
        "\n".join(
            [
                "node_id,node_type",
                "store:1,Store",
                "store:2,Store",
                "community:7,Community",
                "community_report:community_report_7,CommunityReport",
                "extracted_entity:dish:pho,ExtractedEntity",
                "extracted_relation:r1,ExtractedRelation",
            ]
        ),
        encoding="utf-8",
    )
    edges_path.write_text(
        "\n".join(
            [
                "src_id,relation,dst_id",
                "store:1,IN_COMMUNITY,community:7",
                "store:2,IN_COMMUNITY,community:7",
                "community:7,HAS_REPORT,community_report:community_report_7",
            ]
        ),
        encoding="utf-8",
    )

    summary = summarize_graph_csvs(nodes_path, edges_path)

    assert summary["communities"] == 1
    assert summary["community_reports"] == 1
    assert summary["extracted_entities"] == 1
    assert summary["extracted_relations"] == 1
    assert summary["in_community_edges"] == 2
    assert summary["has_report_edges"] == 1
    assert summary["restaurants_with_community"] == 2
