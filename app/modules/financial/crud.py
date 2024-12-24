from fastapi import logger
from sqlalchemy import text
from sqlalchemy.orm import Session


class FinancialCrud:
    def __init__(self, db: Session):
        self.db = db

    def get_financial_ratio_quarters(self, table_name: str, ticker: str, db: Session):
        """부채비율 계산을 위한 4분기 데이터 조회"""
        query = text(f"""
            SELECT Name, total_dept, equity
            FROM {table_name}
            WHERE Code = :ticker
            ORDER BY period_q DESC
            LIMIT 4
        """)

        result = db.execute(query, {"ticker": ticker})
        return result.fetchall()

    def get_liquidity_ratio_quarters(self, table_name: str, ticker: str, db: Session):
        """유동비율 계산을 위한 4분기 데이터 조회"""
        query = text(f"""
            SELECT Name, current_asset, current_dept
            FROM {table_name}
            WHERE Code = :ticker
            ORDER BY period_q DESC
            LIMIT 4
        """)

        result = db.execute(query, {"ticker": ticker})
        return result.fetchall()

    def get_interest_coverage_ratio_quarters(self, table_name: str, ticker: str, db: Session):
        """이자보상배율 계산을 위한 4분기 데이터 조회"""
        query = text(f"""
            SELECT Name, operating_income, fin_cost
            FROM {table_name}
            WHERE Code = :ticker
            ORDER BY period_q DESC
            LIMIT 4
        """)

        result = db.execute(query, {"ticker": ticker})
        return result.fetchall()

    def get_financial_industry_avg_data(
        self, table_name: str, base_ticker: str, is_usa: bool, ratio_type: str, db: Session
    ) -> float:
        """업종 평균 재무비율 조회"""
        ratio_calculations = {
            "debt": """WHEN CAST(f.equity AS DECIMAL) != 0
                      THEN (CAST(f.total_dept AS DECIMAL) / CAST(f.equity AS DECIMAL)) * 100""",
            "liquidity": """WHEN CAST(f.current_dept AS DECIMAL) != 0
                           THEN (CAST(f.current_asset AS DECIMAL) / CAST(f.current_dept AS DECIMAL)) * 100""",
            "interest": """WHEN CAST(f.fin_cost AS DECIMAL) != 0
                            THEN CAST(f.operating_income AS DECIMAL) / CAST(f.fin_cost AS DECIMAL)""",
        }

        query = text(f"""
            WITH sector AS (
                SELECT sector_3
                FROM stock_information
                WHERE ticker = :base_ticker
            ),
            sector_companies AS (
                SELECT
                    CASE
                        WHEN :is_usa THEN CONCAT(si.ticker, '-US')
                        ELSE si.ticker
                    END AS ticker
                FROM stock_information si
                JOIN sector s ON si.sector_3 = s.sector_3
                WHERE si.ticker != :base_ticker
            ),
            company_ratios AS (
                SELECT
                    sc.ticker,
                    AVG(
                        CASE
                            {ratio_calculations[ratio_type]}
                            ELSE 0
                        END
                    ) as avg_ratio
                FROM sector_companies sc
                JOIN {table_name} f ON sc.ticker = f.Code
                GROUP BY sc.ticker
                HAVING COUNT(*) >= 4
            )
            SELECT ROUND(AVG(avg_ratio), 2) as industry_avg
            FROM company_ratios
            WHERE avg_ratio > 0
        """)

        try:
            result = db.execute(query, {"base_ticker": base_ticker, "is_usa": is_usa})
            return result.scalar_one_or_none() or 0.0
        except Exception as e:
            logger.error(f"업종 평균 {ratio_type} 비율 조회 중 오류 발생: {str(e)}")
            return 0.0
