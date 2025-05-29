# SubSIC Comparison Tool

A tool to compare SubSIC codes between TDC and Natwest datasets.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

## Quick Start

1. Create and activate a virtual environment:
```bash
uv venv
source .venv/bin/activate  # On Unix/macOS
# OR
.venv\Scripts\activate  # On Windows
```

2. Install directly from git:
```bash
uv pip install git+<repository-url>
```

3. Place your input files in a `_data` directory:
   - `TDCDummyData.xlsx` - TDC dataset
   - `NatwestDummyDataCSV.csv` - Natwest dataset

4. Run the comparison:
```bash
python -m src.sub_sic_comparison
```

## What it does

The script performs a comprehensive analysis that:

1. **Matches companies by Company Registration Number (CRN)**
2. **Identifies website matches** using domain normalization
3. **Compares SubSIC codes** with both exact and partial matching
4. **Generates comprehensive results** in a single output file

### Matching Types

#### Exact SubSIC Match
- Direct comparison of SubSIC codes between datasets
- Returns `True` if any SubSIC code appears in both lists

#### Partial SubSIC Match  
- Splits SubSIC codes on underscore (`_`) and compares prefixes
- Example: `69109_11104` matches with `69109_11098` (both have prefix `69109`)
- Returns `True` if any prefix from one dataset matches any prefix from the other

#### Website Match
- Normalizes website domains using domain calculator
- Removes `www.` prefixes and standardizes formatting
- Returns `True` if normalized domains match

### Output

- Single results file: `_data/results_combined.xlsx`
- Contains all matched companies with columns:
  - `sub_sic_match` - Boolean for exact SubSIC matches
  - `partial_match` - Boolean for partial SubSIC matches  
  - `website_match` - Boolean for website domain matches

### Summary Statistics

The tool generates two summary reports:

1. **All CRN Matches**: Statistics for all companies matched by CRN
2. **CRN + Website Matches**: Statistics filtered to companies with matching websites

Each summary includes:
- Total companies analyzed
- Exact SubSIC matches and match rate
- Partial SubSIC matches and match rate  
- Website matches (where applicable)

## Input File Format

### TDC Data (`TDCDummyData.xlsx`)
Required columns:
- `Companynumber`
- `TDC_Website`
- `TDC_SubSICs` (comma-separated values)

### Natwest Data (`NatwestDummyDataCSV.csv`)
Required columns:
- `Companynumber`
- `NW_Website`
- `CDD Sub_SIC Code` (comma-separated values) 