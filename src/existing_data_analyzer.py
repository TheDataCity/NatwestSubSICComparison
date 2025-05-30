from pathlib import Path
from typing import List, Optional

import polars as pl

from domain_calculator import domain_calculator


class ExistingDataAnalyzer:
    def __init__(self, input_file: Path) -> None:
        self.input_file = input_file
        self.df: Optional[pl.DataFrame] = None

    def read_data(self) -> None:
        """Read the existing dataset from Excel file."""
        self.df = pl.read_excel(self.input_file)

    def clean_websites(self) -> None:
        """Clean and normalize website columns."""
        if self.df is None:
            raise ValueError("Data not loaded")

        self.df = self.df.with_columns([
            # Clean TDC_Website: lowercase and remove www prefix
            pl.col('TDC_Website').str.to_lowercase().str.strip_chars_start('www.').alias('TDC_Website_clean'),

            # Clean NW_Website: lowercase, remove www prefix, and normalize domain
            pl.col('NW_Website').str.to_lowercase().str.strip_chars_start('www.')
            .map_elements(domain_calculator, return_dtype=pl.Utf8).alias('NW_Website_clean')
        ])

    def fix_tdc_subsics(self) -> None:
        """Fix TDC_SubSICs by removing extra spaces and converting to proper list."""
        if self.df is None:
            raise ValueError("Data not loaded")

        def clean_tdc_subsics(subsics_str: str) -> List[str]:
            """Clean the TDC_SubSICs string and return a proper list."""
            if not subsics_str:
                return []

            # Remove brackets and quotes, then split by comma
            cleaned = subsics_str.strip("[]'\"")
            items = cleaned.split(',')

            # Clean each item: remove quotes, spaces
            result = []
            for item in items:
                clean_item = item.strip().strip("'\"").strip()
                if clean_item:
                    result.append(clean_item)
            return result

        self.df = self.df.with_columns([
            pl.col('TDC_SubSICs')
            .map_elements(clean_tdc_subsics, return_dtype=pl.List(pl.Utf8))
            .alias('TDC_SubSICs_clean')
        ])

    def add_website_match(self) -> None:
        """Add website_match column comparing cleaned websites."""
        if self.df is None:
            raise ValueError("Data not loaded")

        self.df = self.df.with_columns([
            (pl.col('TDC_Website_clean') == pl.col('NW_Website_clean')).alias('website_match')
        ])

    def add_sub_sic_matches(self) -> None:
        """Add sub_sic_match and partial_match columns."""
        if self.df is None:
            raise ValueError("Data not loaded")

        def check_exact_match(tdc_sics: List[str], cdd_sic: str) -> bool:
            """Check if CDD SIC code exists in TDC SubSICs."""
            if not tdc_sics or not cdd_sic:
                return False
            return cdd_sic in tdc_sics

        def check_partial_match(tdc_sics: List[str], cdd_sic: str) -> bool:
            """Check if CDD SIC code prefix matches any TDC SubSIC prefix."""
            if not tdc_sics or not cdd_sic or '_' not in cdd_sic:
                return False

            cdd_prefix = cdd_sic.split('_')[0]
            tdc_prefixes = {sic.split('_')[0] for sic in tdc_sics if '_' in sic}
            return cdd_prefix in tdc_prefixes

        self.df = self.df.with_columns([
            # Exact match
            pl.struct(['TDC_SubSICs_clean', 'CDD_SUB_SIC_CODE'])
            .map_elements(lambda x: check_exact_match(x['TDC_SubSICs_clean'], x['CDD_SUB_SIC_CODE']),
                          return_dtype=pl.Boolean)
            .alias('sub_sic_match_new'),

            # Partial match
            pl.struct(['TDC_SubSICs_clean', 'CDD_SUB_SIC_CODE'])
            .map_elements(lambda x: check_partial_match(x['TDC_SubSICs_clean'], x['CDD_SUB_SIC_CODE']),
                          return_dtype=pl.Boolean)
            .alias('partial_match')
        ])

    def generate_summary(self, summary_type: str, filter_website: bool = False) -> dict:
        """Generate summary statistics."""
        if self.df is None:
            raise ValueError("Data not loaded")

        if filter_website:
            filtered_df = self.df.filter(pl.col('website_match'))
        else:
            filtered_df = self.df

        total = filtered_df.height
        website_matches = filtered_df.filter(pl.col('website_match')).height
        sub_sic_matches = filtered_df.filter(pl.col('sub_sic_match_new')).height
        partial_matches = filtered_df.filter(pl.col('partial_match')).height

        return {
            'summary_type': summary_type,
            'total_records': total,
            'website_matches': website_matches,
            'website_match_rate': (website_matches / total * 100) if total > 0 else 0,
            'sub_sic_matches': sub_sic_matches,
            'sub_sic_match_rate': (sub_sic_matches / total * 100) if total > 0 else 0,
            'partial_matches': partial_matches,
            'partial_match_rate': (partial_matches / total * 100) if total > 0 else 0
        }

    def save_results(self, output_path: Path) -> None:
        """Save the analyzed results to Excel."""
        if self.df is None:
            raise ValueError("Data not loaded")

        # Select final columns for output
        output_df = self.df.select([
            'Companynumber',
            'COMPANY_NAME',
            'TDC_Website',
            'NW_Website',
            'TDC_SubSICs_clean',
            'CDD_SUB_SIC_CODE',
            'website_match',
            'sub_sic_match_new',
            'partial_match'
        ]).rename({'sub_sic_match_new': 'sub_sic_match', 'TDC_SubSICs_clean': 'TDC_SubSICs'})

        output_df.write_excel(output_path)

    def run(self, output_path: Optional[Path] = None) -> dict:
        """Run the complete analysis pipeline."""
        self.read_data()
        self.clean_websites()
        self.fix_tdc_subsics()
        self.add_website_match()
        self.add_sub_sic_matches()

        # Generate summaries for all records and website matches only
        all_summary = self.generate_summary("All Records")
        website_summary = self.generate_summary("Website Matches Only", filter_website=True)

        # Print both summaries
        print("All Records Summary:", all_summary)
        print("Website Matches Only Summary:", website_summary)

        if output_path:
            self.save_results(output_path)

        return {'all_records': all_summary, 'website_matches': website_summary}


def main():
    input_file = Path("_data/results_crn_only.xlsx")  # Update with your file path
    output_file = Path("_data/analyzed_results.xlsx")

    analyzer = ExistingDataAnalyzer(input_file)
    summaries = analyzer.run(output_file)

    # Additional formatted output
    print("\n" + "=" * 50)
    print("ANALYSIS COMPLETE")
    print("=" * 50)

    all_summary = summaries['all_records']
    print(f"\nAll Records Analysis:")
    print(f"  Total records: {all_summary['total_records']}")
    print(f"  Website matches: {all_summary['website_matches']} ({all_summary['website_match_rate']:.1f}%)")
    print(f"  Sub SIC matches: {all_summary['sub_sic_matches']} ({all_summary['sub_sic_match_rate']:.1f}%)")
    print(f"  Partial matches: {all_summary['partial_matches']} ({all_summary['partial_match_rate']:.1f}%)")

    website_summary = summaries['website_matches']
    print(f"\nWebsite Matches Only Analysis:")
    print(f"  Total records: {website_summary['total_records']}")
    print(f"  Website matches: {website_summary['website_matches']} ({website_summary['website_match_rate']:.1f}%)")
    print(f"  Sub SIC matches: {website_summary['sub_sic_matches']} ({website_summary['sub_sic_match_rate']:.1f}%)")
    print(f"  Partial matches: {website_summary['partial_matches']} ({website_summary['partial_match_rate']:.1f}%)")


if __name__ == "__main__":
    main()
