from app.database.crud import database


def activate_stock(ticker: str):
    database._update(
        table="stock_trend",
        sets={"is_activate": 1},
        ticker=ticker,
    )

    database._update(
        table="stock_information",
        sets={"is_activate": 1},
        ticker=ticker,
    )


def deactivate_stock(ticker: str):
    database._update(table="stock_trend", sets={"is_activate": 0}, ticker=ticker)

    database._update(table="stock_information", sets={"is_activate": 0}, ticker=ticker)
