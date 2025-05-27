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
   - `NatwestDummyData.xlsx` - Natwest dataset

4. Run the comparison:
```bash
python -m src.sub_sic_comparison
```

## What it does

The script performs two separate analyses:

### Analysis 1: CRN Match Only
- Matches companies by Company Registration Number (CRN)
- Compares SubSIC codes between datasets
- No website filtering applied
- Saves results to `_data/results_crn_only.xlsx`

### Analysis 2: CRN + Website Match
- Matches companies by CRN
- Filters to only companies with matching website domains
- Compares SubSIC codes for the filtered subset
- Saves results to `_data/results_crn_website.xlsx`

Both analyses generate summary statistics showing:
- Total companies analyzed
- Number of SubSIC matches
- Match rate percentage

## Input File Format

### TDC Data (`TDCDummyData.xlsx`)
Required columns:
- `Companynumber`
- `TDC_Website`
- `TDC_SubSICs` (comma-separated values)

### Natwest Data (`NatwestDummyData.xlsx`)
Required columns:
- `Companynumber`
- `NW_Website`
- `CDD Sub_SIC Code` (comma-separated values) 