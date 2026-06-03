from services.query_parser.parser import QueryParser


def test_query_parser_extracts_basic_fields():
    parser = QueryParser()
    result = parser.parse("tim quan com rang gan bach khoa duoi 50k rating cao")
    assert result["food"] == "com rang"
    assert result["location"] == "bach khoa"
    assert result["max_price"] == 50000


def test_query_parser_accepts_accented_vietnamese():
    parser = QueryParser()
    result = parser.parse("tìm quán cơm gà gần Bách Khoa dưới 50k đánh giá cao")
    assert result["food"] == "com ga"
    assert result["location"] == "bach khoa"
    assert result["max_price"] == 50000
    assert "rating" in result["priority"]
    assert "distance" in result["priority"]
