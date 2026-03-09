from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from .fetch_congress_data import load_billstatus_for_congresses
from .fetch_cbo_estimates import load_cbo_estimates
from .features_structured import fill_structured_defaults
from .join_bills_cbo import join_bill_and_cbo
from .parse_bill_status import parse_billstatus_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_bills(congresses: Iterable[int]) -> pd.DataFrame:
rows = []
for record in load_billstatus_for_congresses(congresses):
parsed = parse_billstatus_record(record)
if parsed:
rows.append(parsed)
logger.info("Parsed %d bill records", len(rows))
return pd.DataFrame(rows)


def main(congress_start: int = 110, congress_end: int = 118) -> None:
congresses = list(range(congress_start, congress_end + 1))
bills_df = parse_bills(congresses)
bills_df = fill_structured_defaults(bills_df)

cbo_df = load_cbo_estimates()
dataset = join_bill_and_cbo(bills_df, cbo_df)

processed_dir = Path("data/processed")
processed_dir.mkdir(parents=True, exist_ok=True)
dataset.to_parquet(processed_dir / "full_history_legislation.parquet", index=False)
dataset.to_csv(processed_dir / "full_history_legislation.csv", index=False)
logger.info("Saved %d joined rows", len(dataset))


if __name__ == "__main__":
main()
