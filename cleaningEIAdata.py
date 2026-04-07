# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 18:27:36 2026

@author: adila
"""

import pandas as pd
import numpy as np
import zipfile
import glob
import re
from pathlib import Path

# INITIAL THINGS

data_dir = Path("C:/EIA Data")   # folder containing yearly EIA zip files
include_territories = False   


# COLUMN TEMPLATES

DR_COLS = [
    "state", "year", "dr_customers", "dr_energy_savings_mwh",
    "dr_potential_peak_mw", "dr_actual_peak_mw",
    "dr_customer_incentives_kusd", "dr_other_costs_kusd"
]

OPS_COLS = [
    "state", "year", "summer_peak_mw", "winter_peak_mw",
    "sales_to_ultimate_customers_mwh"
]

SALES_COLS = [
    "state", "year", "sales_total_revenue_kusd",
    "sales_total_mwh", "sales_total_customers"
]

# FUNCTIONS TO HELP US LATER ON!!

def empty_df(columns):
    return pd.DataFrame(columns=columns)

def standardize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df

def to_numeric_safe(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

def clean_state_col(df):
    if "State" in df.columns:
        df["State"] = df["State"].astype(str).str.strip().str.upper()
    return df

def clean_keys(df):
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip().str.upper()
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df

def get_zip_files(folder):
    return sorted(glob.glob(str(folder / "f861*.zip")))

def find_file_in_zip(zf, pattern):
    matches = [name for name in zf.namelist() if re.search(pattern, name, re.IGNORECASE)]
    return matches[0] if matches else None

def read_excel_from_zip(zip_path, inner_file, sheet_name):
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(inner_file) as f:
            df = pd.read_excel(f, sheet_name=sheet_name, header=2)
    df = standardize_columns(df)
    return df

def get_sheet_list(base_type):
    if include_territories:
        if base_type == "demand_response":
            return ["Demand Response_States", "Demand Response_Territories"]
        elif base_type in ["operational", "sales"]:
            return ["States", "Territories"]
    else:
        if base_type == "demand_response":
            return ["Demand Response_States"]
        elif base_type in ["operational", "sales"]:
            return ["States"]
    return []


# PARSE DEMAND RESPONSE

def parse_demand_response(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        inner = find_file_in_zip(zf, r"Demand_Response_\d{4}\.xlsx$")
        if inner is None:
            print(f"Skipping {Path(zip_path).name}: no Demand Response workbook found")
            return empty_df(DR_COLS)

    dfs = []
    for sheet in get_sheet_list("demand_response"):
        try:
            df = read_excel_from_zip(zip_path, inner, sheet)
            dfs.append(df)
        except Exception as e:
            print(f"Could not read {sheet} in {Path(zip_path).name}: {e}")

    if not dfs:
        return empty_df(DR_COLS)

    df = pd.concat(dfs, ignore_index=True)
    df = clean_state_col(df)

    needed = [
        "Data Year", "State",
        "Total", "Total.1", "Total.2", "Total.3", "Total.4", "Total.5"
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"Demand Response missing columns in {Path(zip_path).name}: {missing}")
        print(df.columns.tolist())
        return empty_df(DR_COLS)

    out = pd.DataFrame({
        "year": to_numeric_safe(df["Data Year"]),
        "state": df["State"],
        "dr_customers": to_numeric_safe(df["Total"]),
        "dr_energy_savings_mwh": to_numeric_safe(df["Total.1"]),
        "dr_potential_peak_mw": to_numeric_safe(df["Total.2"]),
        "dr_actual_peak_mw": to_numeric_safe(df["Total.3"]),
        "dr_customer_incentives_kusd": to_numeric_safe(df["Total.4"]),
        "dr_other_costs_kusd": to_numeric_safe(df["Total.5"])
    })

    out = clean_keys(out)
    out = out.dropna(subset=["state", "year"])

    out = (
        out.groupby(["state", "year"], as_index=False)
        .sum(numeric_only=True)
    )

    return out

# PARSE OPERATIONAL DATA

def parse_operational(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        inner = find_file_in_zip(zf, r"Operational_Data_\d{4}\.xlsx$")
        if inner is None:
            print(f"Skipping {Path(zip_path).name}: no Operational Data workbook found")
            return empty_df(OPS_COLS)

    dfs = []
    for sheet in get_sheet_list("operational"):
        try:
            df = read_excel_from_zip(zip_path, inner, sheet)
            dfs.append(df)
        except Exception as e:
            print(f"Could not read {sheet} in {Path(zip_path).name}: {e}")

    if not dfs:
        return empty_df(OPS_COLS)

    df = pd.concat(dfs, ignore_index=True)
    df = clean_state_col(df)

    needed = ["Data Year", "State", "Summer Peak Demand", "Winter Peak Demand", "Sales to Ultimate Customers"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"Operational missing columns in {Path(zip_path).name}: {missing}")
        print(df.columns.tolist())
        return empty_df(OPS_COLS)

    out = pd.DataFrame({
        "year": to_numeric_safe(df["Data Year"]),
        "state": df["State"],
        "summer_peak_mw": to_numeric_safe(df["Summer Peak Demand"]),
        "winter_peak_mw": to_numeric_safe(df["Winter Peak Demand"]),
        "sales_to_ultimate_customers_mwh": to_numeric_safe(df["Sales to Ultimate Customers"])
    })

    out = clean_keys(out)
    out = out.dropna(subset=["state", "year"])

    out = (
        out.groupby(["state", "year"], as_index=False)
        .sum(numeric_only=True)
    )

    return out

# PARSE SALES TO ULTIMATE CUSTOMERS

def parse_sales(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        inner = find_file_in_zip(zf, r"Sales_Ult_Cust(_CS)?_\d{4}\.xlsx$|Sales_Ult_Cust_\d{4}\.xlsx$")
        if inner is None:
            print(f"Skipping {Path(zip_path).name}: no Sales workbook found")
            return empty_df(SALES_COLS)

    dfs = []
    for sheet in get_sheet_list("sales"):
        try:
            df = read_excel_from_zip(zip_path, inner, sheet)
            dfs.append(df)
        except Exception as e:
            print(f"Could not read {sheet} in {Path(zip_path).name}: {e}")

    if not dfs:
        return empty_df(SALES_COLS)

    df = pd.concat(dfs, ignore_index=True)
    df = clean_state_col(df)

    needed = ["Data Year", "State", "Thousand Dollars.4", "Megawatthours.4", "Count.4"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"Sales missing columns in {Path(zip_path).name}: {missing}")
        print(df.columns.tolist())
        return empty_df(SALES_COLS)

    out = pd.DataFrame({
        "year": to_numeric_safe(df["Data Year"]),
        "state": df["State"],
        "sales_total_revenue_kusd": to_numeric_safe(df["Thousand Dollars.4"]),
        "sales_total_mwh": to_numeric_safe(df["Megawatthours.4"]),
        "sales_total_customers": to_numeric_safe(df["Count.4"])
    })

    out = clean_keys(out)
    out = out.dropna(subset=["state", "year"])

    out = (
        out.groupby(["state", "year"], as_index=False)
        .sum(numeric_only=True)
    )

    return out

# MAIN LOOP

zip_files = get_zip_files(data_dir)

all_dr = []
all_ops = []
all_sales = []

for zp in zip_files:
    print(f"\nProcessing {Path(zp).name}...")

    dr = parse_demand_response(zp)
    ops = parse_operational(zp)
    sales = parse_sales(zp)

    print("  DR shape:", dr.shape)
    print("  OPS shape:", ops.shape)
    print("  SALES shape:", sales.shape)

    if not dr.empty:
        all_dr.append(dr)
    if not ops.empty:
        all_ops.append(ops)
    if not sales.empty:
        all_sales.append(sales)

# CONCATENATE SAFELY

dr_panel = pd.concat(all_dr, ignore_index=True) if all_dr else empty_df(DR_COLS)
ops_panel = pd.concat(all_ops, ignore_index=True) if all_ops else empty_df(OPS_COLS)
sales_panel = pd.concat(all_sales, ignore_index=True) if all_sales else empty_df(SALES_COLS)

dr_panel = clean_keys(dr_panel)
ops_panel = clean_keys(ops_panel)
sales_panel = clean_keys(sales_panel)

print("\nCombined shapes:")
print("dr_panel:", dr_panel.shape)
print("ops_panel:", ops_panel.shape)
print("sales_panel:", sales_panel.shape)

# MERGE

panel = dr_panel.merge(ops_panel, on=["state", "year"], how="outer")
panel = panel.merge(sales_panel, on=["state", "year"], how="outer")

# CREATE NORMALIZED VARIABLES

panel["summer_peak_per_mwh"] = panel["summer_peak_mw"] / panel["sales_total_mwh"]
panel["winter_peak_per_mwh"] = panel["winter_peak_mw"] / panel["sales_total_mwh"]

panel["dr_actual_peak_share_of_summer_peak"] = panel["dr_actual_peak_mw"] / panel["summer_peak_mw"]
panel["dr_potential_peak_share_of_summer_peak"] = panel["dr_potential_peak_mw"] / panel["summer_peak_mw"]

# Additional logs

panel["log_sales_total_mwh"] = np.log(panel["sales_total_mwh"].replace(0, np.nan))
panel["log_summer_peak_mw"] = np.log(panel["summer_peak_mw"].replace(0, np.nan))
panel["log_dr_actual_peak_mw"] = np.log(panel["dr_actual_peak_mw"].replace(0, np.nan))

# SORT + SAVE

panel = panel.sort_values(["state", "year"]).reset_index(drop=True)

output_file = data_dir / "eia861_state_year_panel.csv"
panel.to_csv(output_file, index=False)

print(f"\nSaved file to: {output_file}")
print(panel.head(10))
print(panel.shape)