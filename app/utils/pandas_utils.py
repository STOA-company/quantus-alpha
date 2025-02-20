import pandas as pd
from typing import Dict


def df_to_dict(df: pd.DataFrame) -> Dict[str, Dict]:
    return df.to_dict(orient="records")
