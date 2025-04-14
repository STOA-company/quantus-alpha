from typing import Dict

import pandas as pd


def df_to_dict(df: pd.DataFrame) -> Dict[str, Dict]:
    return df.to_dict(orient="records")
