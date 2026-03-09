param(
[string]$RootPath = "$HOME\Desktop\DebtParadoxResearch"
)

function Write-ProjectFile {
param(
[string]$RelativePath,
[string]$Content
)
$fullPath = Join-Path $RootPath $RelativePath
$dir = Split-Path $fullPath
New-Item -ItemType Directory -Force -Path $dir | Out-Null
Set-Content -LiteralPath $fullPath -Value $Content -Encoding UTF8
}

Write-ProjectFile "etl/__init__.py" @'
__all__ = []
'@

Write-ProjectFile "etl/fetch_congress_data.py" @'
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
'@

Write-ProjectFile "etl/parse_bill_status.py" @'
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

from .fetch_congress_data import choose_best_text_version, fetch_govinfo_xml_text


@dataclass
class BillActionFlags:
passed_house: int = 0
passed_senate: int = 0
enacted_law: int = 0
date_introduced: Optional[str] = None
date_first_passed_house: Optional[str] = None
date_first_passed_senate: Optional[str] = None
date_enacted: Optional[str] = None


def normalize_bill_type(raw_type: str) -> str:
return raw_type.lower().replace(".", "")


def parse_date(date_str: Optional[str]) -> Optional[str]:
if not date_str:
return None
try:
return datetime.fromisoformat(date_str[:10]).date().isoformat()
except (ValueError, TypeError):
return None


def extract_titles(bill: Dict) -> Tuple[Optional[str], Optional[str]]:
titles = bill.get("titles", {}).get("titles", [])
short_title = None
long_title = None
for entry in titles:
title = entry.get("title")
if entry.get("type", "").lower() == "shorttitle" and not short_title:
short_title = title
elif not long_title and entry.get("type", "").lower() == "officialtitle":
long_title = title
if not long_title:
long_title = bill.get("title") or bill.get("titleText")
return long_title, short_title


def extract_summary_text(bill: Dict) -> Optional[str]:
summaries = bill.get("summaries", {}).get("summaries", [])
if not summaries:
return None
sorted_summaries = sorted(summaries, key=lambda s: s.get("date", ""), reverse=True)
return sorted_summaries[0].get("text")


def parse_actions(bill: Dict) -> BillActionFlags:
flags = BillActionFlags()
actions = bill.get("actions", {}).get("actions", [])
for action in actions:
code = action.get("actionCode", "").upper()
action_date = parse_date(action.get("actionDate"))
if code in {"7000", "HINTRO"} and not flags.date_introduced:
flags.date_introduced = action_date
if code in {"36000", "HNUM"} and not flags.date_first_passed_house:
flags.passed_house = 1
flags.date_first_passed_house = action_date
if code in {"37000", "SNUM"} and not flags.date_first_passed_senate:
flags.passed_senate = 1
flags.date_first_passed_senate = action_date
if code in {"7500", "PLAW", "ENACTED"} and not flags.date_enacted:
flags.enacted_law = 1
flags.date_enacted = action_date
if not flags.date_introduced:
intro = bill.get("introducedDate")
flags.date_introduced = parse_date(intro)
return flags


def derive_final_stage(flags: BillActionFlags) -> str:
if flags.enacted_law:
return "enacted"
if flags.passed_house and flags.passed_senate:
return "passed_both"
if flags.passed_house or flags.passed_senate:
return "passed_one_chamber"
return "introduced_only"


def extract_sponsor_info(bill: Dict) -> Tuple[str, str, str]:
sponsor = bill.get("sponsors", {}).get("item") or bill.get("sponsors")
if isinstance(sponsor, dict):
party = sponsor.get("party")
state = sponsor.get("state")
member_id = sponsor.get("bioguideId")
else:
party = state = member_id = None
return party or "", state or "", member_id or ""


def count_cosponsors(bill: Dict) -> Tuple[int, int, int]:
total = dem = rep = 0
cosponsors = bill.get("cosponsors", {}).get("cosponsors", [])
for cos in cosponsors:
total += 1
party = (cos.get("party") or "").upper()
if party == "D":
dem += 1
elif party == "R":
rep += 1
return total, dem, rep


def extract_committees(bill: Dict) -> Tuple[Optional[str], Optional[str]]:
committees = bill.get("committees", {}).get("committees", [])
if not committees:
return None, None
primary = committees[0]
return primary.get("code"), primary.get("chamber")


def extract_policy_subjects(bill: Dict) -> Tuple[Optional[str], Optional[str]]:
policy_area = bill.get("policyArea", {}).get("name")
subjects = bill.get("subjects", {}).get("subjects", [])
subject_terms = "; ".join(term.get("name") for term in subjects if term.get("name"))
return policy_area, subject_terms or None


def parse_billstatus_record(record: Dict) -> Optional[Dict]:
bill = record.get("bill")
if not bill:
return None
congress = int(bill.get("congress"))
bill_type = normalize_bill_type(bill.get("billType"))
bill_number = int(bill.get("number"))
bill_id = f"{congress}-{bill_type}-{bill_number}"

title, short_title = extract_titles(bill)
summary = extract_summary_text(bill)
action_flags = parse_actions(bill)
final_stage = derive_final_stage(action_flags)

sponsor_party, sponsor_state, sponsor_id = extract_sponsor_info(bill)
cos_total, cos_d, cos_r = count_cosponsors(bill)
committee_code, committee_chamber = extract_committees(bill)
policy_area, subject_terms = extract_policy_subjects(bill)

text_url = choose_best_text_version(record)
full_text = fetch_govinfo_xml_text(text_url) if text_url else None

return {
"bill_id": bill_id,
"congress": congress,
"bill_type": bill_type,
"bill_number": bill_number,
"title": title,
"short_title": short_title,
"summary_text": summary,
"full_text": full_text,
"passed_house": action_flags.passed_house,
"passed_senate": action_flags.passed_senate,
"passed_any_chamber": int(action_flags.passed_house or action_flags.passed_senate),
"enacted_law": action_flags.enacted_law,
"final_stage": final_stage,
"date_introduced": action_flags.date_introduced,
"date_first_passed_house": action_flags.date_first_passed_house,
"date_first_passed_senate": action_flags.date_first_passed_senate,
"date_enacted": action_flags.date_enacted,
"sponsor_party": sponsor_party,
"sponsor_state": sponsor_state,
"sponsor_id": sponsor_id,
"cosponsor_count_total": cos_total,
"cosponsor_count_D": cos_d,
"cosponsor_count_R": cos_r,
"primary_committee_code": committee_code,
"primary_committee_chamber": committee_chamber,
"policy_area_str": policy_area,
"subject_terms_str": subject_terms,
}
'@

Write-ProjectFile "etl/fetch_cbo_estimates.py" @'
from __future__ annotations