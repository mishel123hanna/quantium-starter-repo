import re
import numpy as np
import pandas as pd
from pathlib import Path


def clean_numeric(value):
    """Remove currency/whitespace and convert to float.
    Heuristics: if there's a single comma and no dot, treat comma as decimal sep
    when the fractional part looks like <=2 digits; otherwise treat commas as thousands.
    """
    s = str(value).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return np.nan
    # keep digits, dot, comma
    s = re.sub(r"[^\d,.]", "", s)
    if s == "":
        return np.nan
    if s.count(",") == 1 and s.count(".") == 0:
        frac = s.split(",")[-1]
        if len(frac) <= 2:
            s = s.replace(",", ".")  # treat comma as decimal separator
        else:
            s = s.replace(",", "")  # comma as thousand-sep
    else:
        s = s.replace(",", "")  # remove commas
    try:
        return float(s)
    except Exception:
        return np.nan


input_folder = Path("data")
output_folder = Path("processed")
output_folder.mkdir(exist_ok=True)

for file in input_folder.glob("*.csv"):
    df = pd.read_csv(file, low_memory=False)

    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    if (
        "product" not in df.columns
        or "price" not in df.columns
        or "quantity" not in df.columns
    ):
        print(f"Skipping {file.name}: missing required columns")
        continue

    # filter product (case-insensitive)
    mask = df["product"].astype(str).str.strip().str.lower() == "pink morsel"
    filtered = df.loc[mask].copy()

    if filtered.empty:
        print(
            f"No 'Pink Morsel' rows in {file.name}; writing empty output with headers."
        )
        pd.DataFrame(columns=["sales", "date", "region"]).to_csv(
            output_folder / f"processed_{file.name}", index=False
        )
        continue

    # clean numeric columns
    filtered["price_clean"] = filtered["price"].apply(clean_numeric)
    filtered["quantity_clean"] = filtered["quantity"].apply(clean_numeric)

    filtered["sales"] = filtered["price_clean"] * filtered["quantity_clean"]
    filtered["sales"] = filtered["sales"].apply(lambda x: f"${x:.2f}")

    out_df = filtered[["sales", "date", "region"]].copy()

    out_file = output_folder / f"processed_{file.name}"
    out_df.to_csv(out_file, index=False)
    print(f"Processed {file.name}: {len(out_df)} rows -> {out_file}")
