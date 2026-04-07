# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 23:52:00 2026

@author: adila
"""

import pandas as pd

def summary_stats(df, cols):
    stats = []
    for c in cols:
        mean = df[c].mean()
        median = df[c].median()
        std = df[c].std()
        skew = 3 * (mean - median) / std if std != 0 else None
        stats.append([c, mean, median, std, skew])
    return pd.DataFrame(stats, columns=["Variable", "Mean", "Median", "Std Dev", "Skewness"])

cols = [
    "summer_peak_per_mwh",
    "dr_actual_peak_mw",
    "dr_potential_peak_mw",
    "dr_customers",
    "sales_total_mwh",
    "summer_peak_mw"
]

summary_stats(panel, cols)