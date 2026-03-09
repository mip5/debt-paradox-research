from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def load_data(path: Path) -> pd.DataFrame:
df = pd.read_parquet(path)
df["estimate_date"] = pd.to_datetime(df["estimate_date"])
return df


def print_sanity_checks(df: pd.DataFrame) -> None:
print("=== Dataset Overview ===")
print(f"Rows: {len(df):,}")
cols = [
"full_text",
"summary_text",
"deficit_magnitude_bil",
"deficit_sign",
"passed_any_chamber",
]
print("\nNon-null counts:")
print(df[cols].notnull().sum())

print("\nDeficit sign value counts:")
print(df["deficit_sign"].value_counts(dropna=False))

stats = df["deficit_magnitude_bil"].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])
print("\nDeficit magnitude stats (billions):")
print(stats[["min", "0.05", "0.25", "0.5", "0.75", "0.95", "max", "mean", "std"]])

print("\npassed_any_chamber counts:")
print(df["passed_any_chamber"].value_counts(dropna=False))
print("\nenacted_law counts:")
print(df["enacted_law"].value_counts(dropna=False))


def truncate(text: str, max_len: int = 280) -> str:
if text is None:
return ""
text = text.replace("\n", " ").strip()
return text[: max_len - 3] + "..." if len(text) > max_len else text


def sample_examples(df: pd.DataFrame) -> None:
print("\n=== Sample: large positive deficit (> +100) ===")
pos = df[df["deficit_magnitude_bil"] > 100]
if len(pos) > 0:
for _, row in pos.sample(min(5, len(pos)), random_state=42).iterrows():
print(
row["bill_id"],
row["title"],
truncate(row["summary_text"]),
row["deficit_magnitude_bil"],
row["deficit_sign"],
row["passed_any_chamber"],
row["final_stage"],
sep=" | ",
)

print("\n=== Sample: large negative deficit (< -100) ===")
neg = df[df["deficit_magnitude_bil"] < -100]
if len(neg) > 0:
for _, row in neg.sample(min(5, len(neg)), random_state=43).iterrows():
print(
row["bill_id"],
row["title"],
truncate(row["summary_text"]),
row["deficit_magnitude_bil"],
row["deficit_sign"],
row["passed_any_chamber"],
row["final_stage"],
sep=" | ",
)

print("\n=== Sample: never passed any chamber ===")
not_passed = df[df["passed_any_chamber"] == 0]
if len(not_passed) > 0:
for _, row in not_passed.sample(min(5, len(not_passed)), random_state=44).iterrows():
print(
row["bill_id"],
row["title"],
truncate(row["summary_text"]),
row["deficit_magnitude_bil"],
row["deficit_sign"],
row["passed_any_chamber"],
row["final_stage"],
sep=" | ",
)


def build_clean_subset(
df: pd.DataFrame,
min_summary_len: int = 100,
drop_small_impacts: bool = True,
min_abs_magnitude: float = 0.1,
) -> pd.DataFrame:
mask = df["summary_text"].notnull() & (df["summary_text"].str.len() >= min_summary_len)
mask &= df["deficit_magnitude_bil"].notnull() & np.isfinite(df["deficit_magnitude_bil"])
window_len = df["label_window_end"] - df["label_window_start"]
off_window = window_len != 10
print(f"Rows with non-standard budget window: {off_window.sum()}")
if drop_small_impacts:
mask &= df["deficit_magnitude_bil"].abs() >= min_abs_magnitude
clean = df.loc[mask].copy()
print(f"Clean subset rows: {len(clean):,} / {len(df):,}")
return clean


def save_outputs(df: pd.DataFrame, processed_dir: Path) -> None:
processed_dir.mkdir(parents=True, exist_ok=True)
df.to_parquet(processed_dir / "train_ready_legislation.parquet", index=False)
df.to_csv(processed_dir / "train_ready_legislation.csv", index=False)
print("Saved cleaned data to data/processed/train_ready_legislation.*")


def main(args: argparse.Namespace) -> None:
df = load_data(Path(args.full_history_path))
print_sanity_checks(df)
sample_examples(df)
clean = build_clean_subset(
df,
min_summary_len=args.min_summary_len,
drop_small_impacts=not args.keep_small_impacts,
min_abs_magnitude=args.min_abs_magnitude,
)
save_outputs(clean, Path("data/processed"))


if __name__ == "__main__":
parser = argparse.ArgumentParser(description="Inspect and clean training dataset")
parser.add_argument(
"--full-history-path",
default="data/processed/full_history_legislation.parquet",
)
parser.add_argument("--min-summary-len", type=int, default=100, dest="min_summary_len")
parser.add_argument("--keep-small-impacts", action="store_true")
parser.add_argument("--min-abs-magnitude", type=float, default=0.1)
main(parser.parse_args())
