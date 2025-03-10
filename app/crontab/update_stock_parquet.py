from app.utils.factor_utils import factor_utils
from app.cache.factors import factors_cache
from fastapi import HTTPException
import logging
import sys


logger = logging.getLogger(__name__)


def update_parquet(country: str):
    """
    Parameters:
    -----------
    country : str
        국가 코드 ('kr' 또는 'us')
    """
    try:
        if country.lower() == "kr":
            factor_utils.process_kr_factor_data()
        elif country.lower() == "us":
            factor_utils.process_us_factor_data()
        else:
            raise ValueError(f"Unsupported country code: {country}. Supported codes are 'kr' and 'us'.")
            
        factor_utils.archive_parquet(country)
        factors_cache.force_update(country=country)
        return {"message": f"Parquet for {country} updated successfully"}
    except Exception as e:
        logger.error(f"Error updating parquet for {country}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    if len(sys.argv) > 1:
        country = sys.argv[1].lower()
        try:
            result = update_parquet(country)
            print(result["message"])
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    else:
        print("Usage: ./update_parquet.py [country_code]")
        sys.exit(1)