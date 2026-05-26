from __future__ import annotations

from llm_graph_extraction import normalize_extraction


def test_normalize_extraction_builds_entities_and_relations():
    entities, relations = normalize_extraction(
        {
            "entities": [
                {
                    "name": "chỗ để xe",
                    "type": "amenity",
                    "sentiment": "positive",
                    "confidence": 0.9,
                    "evidence": "có chỗ để xe",
                },
                {
                    "name": "nhóm bạn",
                    "type": "audience",
                    "sentiment": "neutral",
                    "confidence": 0.8,
                    "evidence": "đi nhóm bạn",
                },
            ],
            "relations": [
                {
                    "source_entity": "chỗ để xe",
                    "target_entity": "nhóm bạn",
                    "relation_type": "GOOD_FOR",
                    "sentiment": "positive",
                    "confidence": 0.7,
                    "evidence": "phù hợp đi nhóm bạn vì có chỗ để xe",
                }
            ],
        },
        store_key="1",
        review_id="r1",
        text_unit_id="tu1",
    )
    assert set(entities["entity_type"]) == {"amenity", "audience"}
    assert "amenity:cho-de-xe" in set(entities["entity_key"])
    assert relations.iloc[0]["relation_type"] == "GOOD_FOR"
    assert relations.iloc[0]["source_entity_key"] == "amenity:cho-de-xe"
