import polars as pl
from typing import List, Dict

def extract_all_data(extraction: Dict[str, List], cwd: str) -> pl.dataframe.frame.DataFrame:

    def read_file(file_name: str, campus: str) -> pl.dataframe.frame.DataFrame:
        building_name = pl.read_csv(
            file_name,
            has_header=False, 
            n_rows=1,
            truncate_ragged_lines=True
        ).row(0)[0]

        df = pl.read_csv(
                file_name,
                skip_rows=1,
                has_header=True
            )
        
        df = df \
            .with_columns([
                pl.col(this_col).cast(pl.Float64).alias(this_col) 
                for this_col in df.columns 
                if this_col != 'Date'
            ]) \
            .with_columns(
                pl.col('Date').str.to_date(r'%d/%m/%Y').alias('Date'),
                pl.lit(campus).alias('campus'),
                pl.lit(building_name).alias('building_name'),
                pl.lit(file_name).alias('file_name')
            )

        return df
    
    final_df = None

    for campus, files in extraction.items():
        for file in files:
            print(f'Processing file: {cwd}/{campus}/{file}')
            df = read_file(f'{cwd}/{campus}/{file}', campus)
            final_df = df if final_df is None else pl.concat([final_df, df])

    return final_df