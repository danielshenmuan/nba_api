# service/zscore.py
from typing import List, Dict, Optional
import numpy as np
import pandas as pd

# --- Your baseline weights (from the notebook) --------------------------------
# Weighted mean of 9 Cat from 2021-22 season (your notebook values):
DEFAULT_MEAN = [12.44, 4.71, 2.81, 0.90, 0.60, 1.40, 0.46, 0.77, 1.46]
# Weighted stdev of 9 Cat from 2021-22 season:
DEFAULT_STDEV = [6.44, 2.51, 2.08, 0.37, 0.41, 0.92, 0.075, 0.11, 0.90]

# Attempt normalizers from your notebook (for FG% & FT% weighting)
FGA_NORM = 10.213
FTA_NORM = 2.575

NINE_CAT_ORDER = ["PTS","REB","AST","STL","BLK","FG3M","FG_PCT","FT_PCT","TO"]

def nine_cat_frame(box_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select exactly your 9 categories in the order you specified.
    """
    keep = ["PLAYER_NAME"] + NINE_CAT_ORDER
    existing = [c for c in keep if c in box_df.columns]
    return box_df[existing].copy()

def compute_zscore_row(row_vals: List[float],
                       means: List[float],
                       stdevs: List[float],
                       fga: float,
                       fta: float) -> float:
    """
    row_vals: [PTS, REB, AST, STl, BLK, FG3M, FG_PCT, FT_PCT, TO]
    Apply your exact weighting logic:
      - z = (x - mean) / stdev for each category
      - FG% weighted by FGA / FGA_NORM
      - FT% weighted by FTA / FTA_NORM
      - TO weighted by -1 (penalty)
    """
    diff = np.subtract(row_vals, means)                    # (9,)
    z_each = np.divide(diff, stdevs, out=np.zeros_like(diff), where=np.array(stdevs) != 0)

    # weights: [PTS,REB,AST,STL,BLK,FG3M, FG%,   FT%,    TO]
    weights = np.array([
        1, 1, 1, 1, 1, 1,
        (fga / FGA_NORM) if FGA_NORM else 1,
        (fta / FTA_NORM) if FTA_NORM else 1,
        -1
    ], dtype=float)

    adjusted = np.multiply(z_each, weights)
    return float(round(np.sum(adjusted), 2))

def attach_zscores(
    box_df: pd.DataFrame,
    means: Optional[List[float]] = None,
    stdevs: Optional[List[float]] = None
) -> pd.DataFrame:
    """
    Adds 'Z_Score' column to box_df using your 9-cat & weighting rules.
    """
    means = means or DEFAULT_MEAN
    stdevs = stdevs or DEFAULT_STDEV

    nine = nine_cat_frame(box_df)

    z_list = []
    for i in range(len(nine)):
        row = nine.iloc[i]
        # order strictly matches NINE_CAT_ORDER
        row_vals = [row.get(cat, 0) for cat in NINE_CAT_ORDER]
        # Need FGA/FTA for FG%/FT% weights; fall back to 0 if missing
        fga = float(box_df.iloc[i].get("FGA", 0)) if "FGA" in box_df.columns else 0.0
        fta = float(box_df.iloc[i].get("FTA", 0)) if "FTA" in box_df.columns else 0.0
        z = compute_zscore_row(row_vals, means, stdevs, fga=fga, fta=fta)
        z_list.append(z)

    out = box_df.copy()
    out["Z_Score"] = z_list
    return out

# --- (Optional) Recompute baselines from data you fetch now --------------------
def compute_baseline_from_frame(box_df: pd.DataFrame) -> Dict[str, List[float]]:
    """
    Compute mean/stdev vectors from a frame of player games (e.g., across a range).
    Useful if you want a 2024-25 baseline instead of 2021-22.
    Returned dict has 'mean' and 'stdev' arrays in NINE_CAT_ORDER.
    """
    nine = nine_cat_frame(box_df)
    stats = nine[NINE_CAT_ORDER].astype(float)
    means = [float(stats[c].mean()) for c in NINE_CAT_ORDER]
    stdevs = [float(stats[c].std(ddof=1) or 1.0) for c in NINE_CAT_ORDER]
    return {"mean": means, "stdev": stdevs}
