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
