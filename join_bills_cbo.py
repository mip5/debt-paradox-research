from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = [
"bill_id",
"congress",
"bill_type",
"bill_number",
"full_text",
"summary_text",
"title",
"short_title",
"deficit_magnitude_bil",
"deficit_sign",
"label_window_start",
"label_window_end",
"estimate_date",
"passed_house",
"passed_senate",
"passed_any_chamber",
"enacted_law",
"final_stage",
"date_introduced",
"date_first_passed_house",
"date_first_passed_senate",
"date_enacted",
"sponsor_party",
"sponsor_state",
"sponsor_id",
"cosponsor_count_total",
"cosponsor_count_D",
"cosponsor_count_R",
"primary_committee_code",
"primary_committee_chamber",
"policy_area_str",
"subject_terms_str",
]


def join_bill_and_cbo(bills_df: pd.DataFrame, cbo_df: pd.DataFrame) -> pd.DataFrame:
merged = bills_df.merge(
cbo_df,
on=["bill_id", "congress", "bill_type", "bill_number"],
how="inner",
validate="one_to_one",
)
missing = [col for col in REQUIRED_COLUMNS if col not in merged.columns]
if missing:
raise ValueError(f"Missing required columns after join: {missing}")
merged = merged.sort_values("estimate_date").reset_index(drop=True)
return merged
