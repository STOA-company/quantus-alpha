def activate_stock(self, tickers: list[str]):
    self.db._update(
        table="stock_information",
        sets={"is_activate": True},
        ticker__in=tickers,
    )


def deactivate_stock(self, tickers: list[str]):
    self.db._update(
        table="stock_information",
        sets={"is_activate": False},
        ticker__in=tickers,
    )
