from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

logger = logging.getLogger(__name__)
CBO_API_ROOT = "https://www.cbo.gov/api/cost-estimates"
PAGE_SIZE = 100


@dataclass
class CBOEstimate:
bill_id: str
congress: int
bill_type: str
bill_number: int
estimate_date: str
label_window_start: Optional[int]
label_window_end: Optional[int]
deficit_magnitude_bil: float
deficit_sign: int


def normalize_bill_keys(item: Dict) -> Optional[Tuple[int, str, int]]:
try:
bill = item["bill_number"]
congress = int(bill["congress"])
bill_type = bill["bill_type"].lower()
number = int(bill["bill_number"])
return congress, bill_type, number
except (KeyError, TypeError, ValueError):
return None


def fetch_cbo_pages(max_pages: Optional[int] = None) -> Iterable[Dict]:
page = 0
while True:
params = {"items_per_page": PAGE_SIZE, "page": page}
resp = requests.get(CBO_API_ROOT, params=params, timeout=60)
resp.raise_for_status()
data = resp.json()
results = data.get("data", [])
if not results:
break
for item in results:
yield item
page += 1
if max_pages and page >= max_pages:
break


def parse_cbo_item(item: Dict) -> Optional[CBOEstimate]:
key = normalize_bill_keys(item)
if not key:
return None
congress, bill_type, bill_number = key
bill_id = f"{congress}-{bill_type}-{bill_number}"
estimate_date = item.get("date", "")[:10]
window = item.get("budgetary_effect", {})
start_year = window.get("start_year")
end_year = window.get("end_year")
net_cost = window.get("value")
if net_cost is None:
return None
magnitude = float(net_cost) / 1e9
if magnitude > 1.0:
deficit_sign = 1
elif magnitude < -1.0:
deficit_sign = -1
else:
deficit_sign = 0
return CBOEstimate(
bill_id=bill_id,
congress=congress,
bill_type=bill_type,
bill_number=bill_number,
estimate_date=estimate_date,
label_window_start=start_year,
label_window_end=end_year,
deficit_magnitude_bil=magnitude,
deficit_sign=deficit_sign,
)


def load_cbo_estimates(max_pages: Optional[int] = None) -> pd.DataFrame:
estimates: List[Dict] = []
for item in fetch_cbo_pages(max_pages=max_pages):
est = parse_cbo_item(item)
if est:
estimates.append(est.__dict__)
logger.info("Fetched %d CBO estimates", len(estimates))
return pd.DataFrame(estimates)
