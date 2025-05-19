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
            pl.col('TDC_SubSICs').str.split(',').alias('TDC_SubSICs')
        ])

        self.nw_df = pl.read_excel(self.nw_file).with_columns([
            pl.col('CDD Sub_SIC Code').str.split(',').alias('CDD_SubSICs')
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

    def match_by_website(self) -> None:
        if self.matched_df is None:
            raise ValueError("No CRN matched data")
            
        # Filter out rows without websites
        self.matched_df = self.matched_df.filter(
            (pl.col('NW_Website').is_not_null())
        )

        # Remove www. prefix from both website columns
        self.matched_df = self.matched_df.with_columns([
            pl.col("TDC_Website").str.strip_chars_start('www.'),
            + pl.col("NW_Website").str.strip_chars_start('www.')
        ])
            
        # Normalize nw website
        self.matched_df = self.matched_df.with_columns([
            pl.col('NW_Website').map_elements(domain_calculator).alias('NW_Website_normalized')
        ])
        
        # Compare websites
        self.matched_df = self.matched_df.with_columns([
            (pl.col('TDC_Website') == pl.col('NW_Website_normalized')).alias('website_match')
        ])

    def compare_sub_sic(self) -> None:
        if self.matched_df is None:
            raise ValueError("No matched data")

        def check_sub_sic_match(our_sics: List[str], nw_sics: List[str]) -> bool:
            if not our_sics or not nw_sics:
                return False
            return any(sic in our_sics for sic in nw_sics)

        self.matched_df = self.matched_df.with_columns([
            pl.struct(['TDC_SubSICs', 'CDD_SubSICs'])
            .map_elements(lambda x: check_sub_sic_match(x['TDC_SubSICs'], x['CDD_SubSICs']))
            .alias('sub_sic_match')
        ])

    def generate_summary(self) -> Dict[str, Any]:
        if self.matched_df is None:
            raise ValueError("No matched data")

        return {
            'total_companies': self.matched_df.height,
            'crn_matches': self.matched_df.height,
            'website_matches': self.matched_df.filter(pl.col('website_match')).height,
            'sub_sic_matches': self.matched_df.filter(pl.col('sub_sic_match')).height,
            'match_rate': (self.matched_df.filter(pl.col('sub_sic_match')).height / self.matched_df.height) * 100
        }

    def save_results(self, output_path: Path) -> None:
        if self.matched_df is None:
            raise ValueError("No matched data")

        # Drop nw sub-sic codes and normalized website
        result_df = self.matched_df.drop(['CDD_SubSICs', 'NW_Website_normalized'])
        result_df.write_excel(output_path)

    def run(self) -> None:
        self.read_data()
        self.match_by_crn()
        self.match_by_website()
        self.compare_sub_sic()

        summary = self.generate_summary()
        print("Summary:", summary)

        output_path = Path("_data/results.xlsx")
        self.save_results(output_path)


def main():
    our_file = Path("_data/TDCDummyData.xlsx")
    nw_file = Path("_data/NatwestDummyData.xlsx")

    comparison = SubSICComparison(our_file, nw_file)
    comparison.run()


if __name__ == "__main__":
    main()
