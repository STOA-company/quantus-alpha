from typing import Optional, Dict, List, Any
from datetime import date
from app.modules.common.services import CommonService, get_common_service
from fastapi import HTTPException, Depends
import logging
from app.database.crud import database
from app.modules.common.enum import Country
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class FinancialDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[{
            "code": "005930",
            "name": "삼성전자",
            "period": "2023Q4",
            "revenue": 1000000000000,
            "operating_income": 100000000000,
            "net_income": 80000000000,
            "gross_profit": 300000000000,
            "operating_margin": 10.0,
            "net_margin": 8.0,
            "rnd_ratio": 8.5,
            "yoy_revenue_growth": 5.2,
            "yoy_operating_income_growth": 7.1,
            "yoy_net_income_growth": 6.8,
            "currency": "KRW"
        }]
    )

class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.common_service = common_service
        self.income_tables = {
            Country.KR: "KOR_income",
            Country.US: "USA_income"
        }

    def _convert_row_to_dict(self, row, ctry: Country) -> Dict[str, Any]:
        """SQLAlchemy Row를 딕셔너리로 변환"""
        try:
            return {
                'code': str(row.code),
                'name': str(row.name),
                'period': str(row.period_q),
                'revenue': float(row.rev or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'operating_income': float(row.operating_income or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'net_income': float(row.net_income or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'gross_profit': float(row.gross_profit or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'operating_margin': round(float(row.operating_income or 0) / float(row.rev or 1) * 100, 2),
                'net_margin': round(float(row.net_income or 0) / float(row.rev or 1) * 100, 2),
                'rnd_ratio': round(float(row.rnd_expense or 0) / float(row.rev or 1) * 100, 2),
                'currency': 'USD' if ctry == Country.US else 'KRW'
            }
        except Exception as e:
            logger.error(f"Error converting row: {e}")
            logger.debug(f"Row data: {row}")
            raise

    async def read_financial_data(
        self, 
        ctry: Country, 
        ticker: str, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> FinancialDataResponse:
        """
        국가별 재무제표 데이터를 조회하고 반환합니다.
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            conditions = {
                "Code": ticker
            }
            
            if start_date:
                conditions["period_q__gte"] = start_date.strftime("%Y")
            if end_date:
                conditions["period_q__lte"] = end_date.strftime("%Y")

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No data found for ticker {ticker} in {ctry.value.upper()}"
                )

            columns = ['Code', 'Name', 'period_q', 'rev', 'cost_of_sales', 'gross_profit', 
                      'sell_admin_cost', 'rnd_expense', 'operating_income', 'other_rev_gains', 
                      'other_exp_losses', 'equity method gain', 'fin_profit', 'fin_cost', 
                      'pbt', 'corp_tax_cost', 'profit_continuing_ops', 'net_income_total', 
                      'net_income', 'net_income_not_control']
            
            financial_data = []
            for row in result:
                row_dict = dict(zip(columns, row))
                financial_data.append({
                    "code": row_dict["Code"],
                    "name": row_dict["Name"],
                    "period": row_dict["period_q"],
                    "revenue": row_dict["rev"],
                    "costOfSales": row_dict["cost_of_sales"],
                    "grossProfit": row_dict["gross_profit"],
                    "sellAdminCost": row_dict["sell_admin_cost"],
                    "rndExpense": row_dict["rnd_expense"],
                    "operatingIncome": row_dict["operating_income"],
                    "otherRevGains": row_dict["other_rev_gains"],
                    "otherExpLosses": row_dict["other_exp_losses"],
                    "equityMethodGain": row_dict["equity method gain"],
                    "finProfit": row_dict["fin_profit"],
                    "finCost": row_dict["fin_cost"],
                    "pbt": row_dict["pbt"],
                    "corpTaxCost": row_dict["corp_tax_cost"],
                    "profitContinuingOps": row_dict["profit_continuing_ops"],
                    "netIncomeTotal": row_dict["net_income_total"],
                    "netIncome": row_dict["net_income"],
                    "netIncomeNotControl": row_dict["net_income_not_control"]
                })

            return FinancialDataResponse(
                data=financial_data
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Internal server error: {str(e)}"
            )

    async def get_latest_quarter(self, ctry: Country, ticker: str) -> str:
        """가장 최근 분기 데이터 조회"""
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            result = self.db._select(
                table=table_name,
                columns=['period_q'],
                order='period_q',
                ascending=False,
                limit=1,
                code=ticker
            )
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No data found for {ticker}"
                )
                
            return result[0][0]
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting latest quarter: {e}")
            raise HTTPException(
                status_code=500, 
                detail=f"Internal server error: {str(e)}"
            )


    # async def read_financial_data(self, data_type: str, ctry: str, ticker: str):
    #     file_path = os.path.join(settings.DATA_DIR, ctry, "financial_updated", data_type, f"{ticker}.parquet")
    #     return await self.common_service.read_local_file(file_path)


def get_financial_service(
    common_service: CommonService = Depends(get_common_service)
) -> FinancialService:
    return FinancialService(common_service=common_service)