from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

import requests
from lxml import etree

BILLSTATUS_ZIP = (
"https://www.congress.gov/bill-status/bulk-data/BILLSTATUS/"
"{congress}/BILLSTATUS-{congress}-{bill_type}.zip"
)

logger = logging.getLogger(__name__)
BILL_TYPES = ["hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres"]


def download_bill_status_zip(congress: int, bill_type: str, dest_dir: Path) -> Path:
dest_dir.mkdir(parents=True, exist_ok=True)
url = BILLSTATUS_ZIP.format(congress=congress, bill_type=bill_type.lower())
out_path = dest_dir / f"BILLSTATUS-{congress}-{bill_type.lower()}.zip"
if out_path.exists():
logger.info("BillStatus ZIP already present: %s", out_path)
return out_path

logger.info("Downloading BillStatus zip %s", url)
resp = requests.get(url, timeout=120)
resp.raise_for_status()
out_path.write_bytes(resp.content)
return out_path


def extract_bill_status_zip(zip_path: Path, extract_root: Path) -> Path:
extract_dir = extract_root / zip_path.stem
if extract_dir.exists():
return extract_dir

logger.info("Extracting %s -> %s", zip_path.name, extract_dir)
extract_dir.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(zip_path, "r") as zf:
zf.extractall(extract_dir)
return extract_dir


def iter_billstatus_json(extracted_dir: Path) -> Iterator[Dict]:
for json_path in extracted_dir.rglob("*.json"):
with json_path.open("r", encoding="utf-8") as fh:
try:
yield json.load(fh)
except json.JSONDecodeError as exc:
logger.warning("Failed to parse %s: %s", json_path, exc)


def load_billstatus_for_congresses(
congresses: Iterable[int],
bill_types: Iterable[str] = BILL_TYPES,
cache_dir: Path = Path("data/raw/billstatus"),
) -> Iterator[Dict]:
extract_root = cache_dir / "extracted"
for congress in congresses:
for bill_type in bill_types:
zip_path = download_bill_status_zip(congress, bill_type, cache_dir)
extracted_dir = extract_bill_status_zip(zip_path, extract_root)
yield from iter_billstatus_json(extracted_dir)


def fetch_govinfo_xml_text(xml_url: str) -> Optional[str]:
if not xml_url:
return None
try:
resp = requests.get(xml_url, timeout=120)
resp.raise_for_status()
except requests.RequestException as exc:
logger.warning("Failed to download full text %s: %s", xml_url, exc)
return None

try:
root = etree.fromstring(resp.content)
text_nodes = root.xpath("//legis-body//text()")
if not text_nodes:
text_nodes = root.xpath("//text()")
return " ".join(t.strip() for t in text_nodes if t.strip())
except etree.XMLSyntaxError as exc:
logger.warning("Bad XML at %s: %s", xml_url, exc)
return None


def choose_best_text_version(bill_json: Dict) -> Optional[str]:
versions = bill_json.get("bill", {}).get("textVersions", {}).get("textVersions", [])
preferred_formats = ("XML", "xml", "Text", "HTML")
for version in versions:
for fmt in version.get("formats", []):
fmt_type = fmt.get("type", "").upper()
if fmt_type in preferred_formats and fmt.get("url"):
return fmt["url"]
return None
