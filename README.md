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

The script will:
- Match companies by CRN
- Compare websites
- Compare SubSIC codes
- Generate a summary of matches
- Save results to `_data/results.xlsx`

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