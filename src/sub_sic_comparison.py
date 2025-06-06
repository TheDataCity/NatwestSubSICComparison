from pathlib import Path
from typing import Dict, Any, Optional, List

import polars as pl

from domain_calculator import domain_calculator


class SubSICComparison:
    def __init__(self, our_file: Path, nw_file: Path) -> None:
        self.our_file = our_file
        self.nw_file = nw_file
        self.our_df: Optional[pl.DataFrame] = None
        self.nw_df: Optional[pl.DataFrame] = None
        self.matched_df: Optional[pl.DataFrame] = None

    def read_data(self) -> None:
        def clean_tdc_subsics(subsics_str: str) -> List[str]:
            """Clean the TDC_SubSICs string and return a proper list."""
            if not subsics_str:
                return []
            
            # Handle both comma-separated strings and string representations of lists
            if subsics_str.startswith('[') and subsics_str.endswith(']'):
                # String representation of list: ['item1', ' item2', ' item3']
                cleaned = subsics_str.strip("[]'\"")
                items = cleaned.split(',')
                
                result = []
                for item in items:
                    clean_item = item.strip().strip("'\"").strip()
                    if clean_item:
                        result.append(clean_item)
                return result
            else:
                # Regular comma-separated string
                items = subsics_str.split(',')
                return [item.strip() for item in items if item.strip()]

        # Read data and convert SubSIC columns to lists
        self.our_df = pl.read_excel(self.our_file).with_columns([
            pl.col('Companynumber').cast(pl.Utf8),
            pl.col('TDC_Website').cast(pl.Utf8),
            pl.col('TDC_SubSICs').cast(pl.Utf8).map_elements(clean_tdc_subsics, return_dtype=pl.List(pl.Utf8)).alias('TDC_SubSICs')
        ])

        self.nw_df = pl.read_csv(self.nw_file, schema_overrides={
            'Companynumber': pl.Utf8,
            'NW_Website': pl.Utf8,
            'CDD Sub_SIC Code': pl.Utf8
        }, encoding='utf8-lossy').with_columns([
            pl.col('CDD Sub_SIC Code').str.split(',').list.eval(pl.element().str.strip_chars()).alias('CDD_SubSICs')
        ])

    def match_by_crn(self) -> None:
        if self.our_df is None or self.nw_df is None:
            raise ValueError("Data not loaded")

        # Join on Companynumber
        self.matched_df = self.our_df.join(
            self.nw_df,
            left_on='Companynumber',
            right_on='Companynumber',
            how='inner',
            suffix='_NW'
        )

    def match_by_website(self) -> pl.DataFrame:
        if self.matched_df is None:
            raise ValueError("No CRN matched data")

        # Start with all matched data
        result_df = self.matched_df.with_columns([
            pl.lit(False).alias('website_match')  # Default to False
        ])

        # Filter rows that have websites for processing
        website_filtered_df = self.matched_df.filter(
            (pl.col('NW_Website').is_not_null()) &
            (pl.col('TDC_Website').is_not_null())
        )

        if website_filtered_df.height > 0:
            # Remove www. prefix from both website columns
            website_filtered_df = website_filtered_df.with_columns([
                pl.col("TDC_Website").str.strip_chars_start('www.'),
                pl.col("NW_Website").str.strip_chars_start('www.').str.to_lowercase()
            ])

            # Normalize nw website
            website_filtered_df = website_filtered_df.with_columns([
                pl.col('NW_Website').map_elements(domain_calculator, return_dtype=pl.Utf8).alias(
                    'NW_Website_normalized')
            ])

            # Compare websites
            website_filtered_df = website_filtered_df.with_columns([
                (pl.col('TDC_Website') == pl.col('NW_Website_normalized')).alias('website_match')
            ])

            # Update the result with website matches
            result_df = result_df.with_columns([
                pl.when(
                    pl.col('Companynumber').is_in(
                        website_filtered_df.filter(pl.col('website_match')).get_column('Companynumber').to_list()
                    )
                ).then(True).otherwise(pl.col('website_match')).alias('website_match')
            ])

        return result_df

    def compare_sub_sic(self, df: pl.DataFrame) -> pl.DataFrame:
        def check_sub_sic_match(our_sics: List[str], nw_sics: List[str]) -> bool:
            if not our_sics or not nw_sics:
                return False
            return any(sic in our_sics for sic in nw_sics)

        def check_partial_sub_sic_match(our_sics: List[str], nw_sics: List[str]) -> bool:
            if not our_sics or not nw_sics:
                return False

            # Extract prefixes from our_sics (split on underscore, take first part)
            our_prefixes = {sic.split('_')[0] for sic in our_sics if '_' in sic}

            # Extract prefixes from nw_sics and check for matches
            for nw_sic in nw_sics:
                if '_' in nw_sic:
                    nw_prefix = nw_sic.split('_')[0]
                    if nw_prefix in our_prefixes:
                        return True
            return False

        return df.with_columns([
            pl.struct(['TDC_SubSICs', 'CDD_SubSICs'])
            .map_elements(lambda x: check_sub_sic_match(x['TDC_SubSICs'], x['CDD_SubSICs']), return_dtype=pl.Boolean)
            .alias('sub_sic_match'),
            pl.struct(['TDC_SubSICs', 'CDD_SubSICs'])
            .map_elements(lambda x: check_partial_sub_sic_match(x['TDC_SubSICs'], x['CDD_SubSICs']),
                          return_dtype=pl.Boolean)
            .alias('partial_match')
        ])

    def generate_summary(self, df: pl.DataFrame, analysis_type: str) -> Dict[str, Any]:
        summary = {
            'analysis_type': analysis_type,
            'total_companies': df.height,
            'sub_sic_matches': df.filter(pl.col('sub_sic_match')).height,
            'match_rate': (df.filter(pl.col('sub_sic_match')).height / df.height) * 100 if df.height > 0 else 0
        }

        if 'partial_match' in df.columns:
            summary['partial_matches'] = df.filter(pl.col('partial_match')).height
            summary['partial_match_rate'] = (df.filter(
                pl.col('partial_match')).height / df.height) * 100 if df.height > 0 else 0

        if 'website_match' in df.columns:
            summary['website_matches'] = df.filter(pl.col('website_match')).height

        return summary

    def save_results(self, df: pl.DataFrame, output_path: Path) -> None:
        # Drop columns that might not exist in all dataframes
        columns_to_drop = []
        for col in ['CDD_SubSICs', 'NW_Website_normalized', 'CDD Sub_SIC Code', 'CDD_SUB_SIC_CODE', 'NW_Website']:
            if col in df.columns:
                columns_to_drop.append(col)

        result_df = df.drop(columns_to_drop) if columns_to_drop else df
        result_df.write_excel(output_path)

    def run(self) -> None:
        self.read_data()
        self.match_by_crn()

        # Get all matched data with website match indicator
        all_matched_df = self.match_by_website()

        # Add sub SIC comparison
        final_df = self.compare_sub_sic(all_matched_df)

        # Generate summaries for reporting
        crn_summary = self.generate_summary(final_df, "All CRN Matches")
        website_only_summary = self.generate_summary(
            final_df.filter(pl.col('website_match')),
            "CRN + Website Matches"
        )

        print("All CRN Matches Summary:", crn_summary)
        print("CRN + Website Matches Summary:", website_only_summary)

        # Save single result file
        self.save_results(final_df, Path("_data/results_combined.xlsx"))


def main():
    our_file = Path("_data/TDCDummyData.xlsx")
    nw_file = Path("_data/NatwestDummyDataCSV.csv")

    comparison = SubSICComparison(our_file, nw_file)
    comparison.run()


if __name__ == "__main__":
    main()
