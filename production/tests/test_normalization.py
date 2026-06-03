from pipelines.normalization.normalize_restaurants import normalize_text


def test_normalize_text_compacts_spaces():
    assert normalize_text("  Com   Ga  ") == "com ga"
