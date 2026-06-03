from __future__ import annotations

import pandas as pd


class PostgresWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def write_dataframe(self, table_name: str, frame: pd.DataFrame) -> dict[str, object]:
        return {
            "status": "stubbed",
            "dsn": self.dsn,
            "table_name": table_name,
            "rows": int(len(frame)),
        }
