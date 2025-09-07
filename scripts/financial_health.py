import pandas as pd
from datetime import datetime

excel_file = 'SmallEV_Qtr_Performance_FullDigits.xlsx' # Path to Excel file

try:
    df_pnl_raw = pd.read_excel(excel_file, sheet_name='Income Statement', header=0)
    df_bs_raw = pd.read_excel(excel_file, sheet_name='Balance Sheet', header=0)
    df_cf_raw = pd.read_excel(excel_file, sheet_name='Cash Flow Statement', header=0)
    print("Excel sheets read successfully.")
except FileNotFoundError:
    print(f"Error: File not found at {excel_file}")
    exit()
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit()

# Data Cleaning
accounts_to_keep_pnl = ~df_pnl_raw['Account'].str.contains('Total|Net Income|Gross Profit|Operating Income|Income Before Tax', case=False, na=True)
accounts_to_keep_bs = ~df_bs_raw['Account'].str.contains('Total', case=False, na=True)
accounts_to_keep_cf = ~df_cf_raw['Account'].str.contains('Net Change', case=False, na=True)

df_pnl = df_pnl_raw[df_pnl_raw['Account'].notna() & accounts_to_keep_pnl].copy()
df_bs = df_bs_raw[df_bs_raw['Account'].notna() & accounts_to_keep_bs].copy()
df_cf = df_cf_raw[df_cf_raw['Account'].notna() & accounts_to_keep_cf].copy()

# Identify Value Columns (Years + Quarters)
value_cols = [col for col in df_pnl.columns if col != 'Account']

# Transform from Wide to Long using melt
pnl_long = pd.melt(df_pnl, id_vars=['Account'], value_vars=value_cols, var_name='Period', value_name='Amount')
pnl_long['StatementType'] = 'Income Statement'

bs_long = pd.melt(df_bs, id_vars=['Account'], value_vars=value_cols, var_name='Period', value_name='Amount')
bs_long['StatementType'] = 'Balance Sheet'

cf_long = pd.melt(df_cf, id_vars=['Account'], value_vars=value_cols, var_name='Period', value_name='Amount')
cf_long['StatementType'] = 'Cash Flow Statement'

# Combine into one DataFrame
df_financials_long = pd.concat([pnl_long, bs_long, cf_long], ignore_index=True)

# Extract Year, Quarter, and create a Date
def get_end_of_quarter_date(period_str):
    # Function to parse 'YYYY QX' format and return end-of-quarter date
    try:
        parts = period_str.split(' ')
        year = int(parts[0])
        quarter = int(parts[1][1]) # Get the number after 'Q'
        month = quarter * 3 # End month of the quarter (3, 6, 9, 12)
        # Get the last day of that month
        next_month_start = datetime(year + (1 if month == 12 else 0), (month % 12) + 1, 1)
        last_day_of_month = (next_month_start - pd.Timedelta(days=1)).date()
        return last_day_of_month
    except:
        return pd.NaT # Return Not-a-Time for parsing errors

df_financials_long['ReportDate'] = df_financials_long['Period'].apply(get_end_of_quarter_date)
df_financials_long['ReportDate'] = pd.to_datetime(df_financials_long['ReportDate'], errors='coerce')

df_financials_long['Year'] = df_financials_long['ReportDate'].dt.year
df_financials_long['Quarter'] = df_financials_long['ReportDate'].dt.quarter

# Clean up Data Types and Handle Missing Values
df_financials_long['Amount'] = pd.to_numeric(df_financials_long['Amount'], errors='coerce')
df_financials_long.dropna(subset=['Amount', 'ReportDate'], inplace=True) # Drop rows where key data is missing
# Convert Year and Quarter to Integer after handling NaT
df_financials_long['Year'] = df_financials_long['Year'].astype(int)
df_financials_long['Quarter'] = df_financials_long['Quarter'].astype(int)


# Final DataFrame Structure
df_final = df_financials_long[['ReportDate', 'Year', 'Quarter', 'StatementType', 'Account', 'Amount']].copy()

print("\nData after transformation to long format:")
print(df_final.head())
print(f"\nTotal rows: {len(df_final)}")
print(f"\nDate range: {df_final['ReportDate'].min()} to {df_final['ReportDate'].max()}")

# Save the transformed data to a new CSV
df_final.to_csv('financials_quarterly_long.csv', index=False)