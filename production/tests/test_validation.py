from pathlib import Path

from pipelines.validation.validate_csv import validate_csv_against_schema


def test_validation_on_befood_csv():
    root = Path(__file__).resolve().parents[1]
    result = validate_csv_against_schema(
        root.parent / "Utils" / "befood_bachkhoa_restaurants.csv",
        root / "data_contracts" / "befood_restaurants.schema.yaml",
    )
    assert result["valid"] is True
