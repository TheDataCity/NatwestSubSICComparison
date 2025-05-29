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
        # Read data and convert SubSIC columns to lists
        self.our_df = pl.read_excel(self.our_file).with_columns([
            pl.col('Companynumber').cast(pl.Utf8),
            pl.col('TDC_Website').cast(pl.Utf8),
            pl.col('TDC_SubSICs').cast(pl.Utf8).str.split(',').list.eval(pl.element().str.strip_chars()).alias(
                'TDC_SubSICs')
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

        # Filter out rows without websites
        website_filtered_df = self.matched_df.filter(
            (pl.col('NW_Website').is_not_null())
        )

        # Remove www. prefix from both website columns
        website_filtered_df = website_filtered_df.with_columns([
            pl.col("TDC_Website").str.strip_chars_start('www.'),
            pl.col("NW_Website").str.strip_chars_start('www.').str.to_lowercase()
        ])

        # Normalize nw website
        website_filtered_df = website_filtered_df.with_columns([
            pl.col('NW_Website').map_elements(domain_calculator, return_dtype=pl.Utf8).alias('NW_Website_normalized')
        ])

        # Compare websites
        website_filtered_df = website_filtered_df.with_columns([
            (pl.col('TDC_Website') == pl.col('NW_Website_normalized')).alias('website_match')
        ])

        # Return only rows where websites match
        return website_filtered_df.filter(pl.col('website_match'))

    def compare_sub_sic(self, df: pl.DataFrame) -> pl.DataFrame:
        def check_sub_sic_match(our_sics: List[str], nw_sics: List[str]) -> bool:
            if not our_sics or not nw_sics:
                return False
            return any(sic in our_sics for sic in nw_sics)

        return df.with_columns([
            pl.struct(['TDC_SubSICs', 'CDD_SubSICs'])
            .map_elements(lambda x: check_sub_sic_match(x['TDC_SubSICs'], x['CDD_SubSICs']), return_dtype=pl.Boolean)
            .alias('sub_sic_match')
        ])

    def generate_summary(self, df: pl.DataFrame, analysis_type: str) -> Dict[str, Any]:
        summary = {
            'analysis_type': analysis_type,
            'total_companies': df.height,
            'sub_sic_matches': df.filter(pl.col('sub_sic_match')).height,
            'match_rate': (df.filter(pl.col('sub_sic_match')).height / df.height) * 100 if df.height > 0 else 0
        }

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

        # Analysis 1: CRN match only (no website filtering)
        crn_only_df = self.compare_sub_sic(self.matched_df)
        crn_summary = self.generate_summary(crn_only_df, "CRN Match Only")
        print("CRN Only Summary:", crn_summary)

        # Analysis 2: CRN + Website match
        website_matched_df = self.match_by_website()
        crn_website_df = self.compare_sub_sic(website_matched_df)
        website_summary = self.generate_summary(crn_website_df, "CRN + Website Match")
        print("CRN + Website Summary:", website_summary)

        # Save both results
        self.save_results(crn_only_df, Path("_data/results_crn_only.xlsx"))
        self.save_results(crn_website_df, Path("_data/results_crn_website.xlsx"))


def main():
    our_file = Path("_data/TDCDummyData.xlsx")
    nw_file = Path("_data/NatwestDummyDataCSV.csv")

    comparison = SubSICComparison(our_file, nw_file)
    comparison.run()


if __name__ == "__main__":
    main()
