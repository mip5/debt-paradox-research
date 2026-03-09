from __future__ import annotations

import pandas as pd


def fill_structured_defaults(df: pd.DataFrame) -> pd.DataFrame:
df = df.copy()
defaults = {
"summary_text": "",
"full_text": "",
"short_title": None,
"primary_committee_code": None,
"primary_committee_chamber": None,
"policy_area_str": None,
"subject_terms_str": None,
}
for col, default in defaults.items():
if col in df.columns:
df[col] = df[col].fillna(default)
numeric_cols = [
"cosponsor_count_total",
"cosponsor_count_D",
"cosponsor_count_R",
"passed_house",
"passed_senate",
"passed_any_chamber",
"enacted_law",
]
for col in numeric_cols:
if col in df.columns:
df[col] = df[col].fillna(0).astype(int)
return df
