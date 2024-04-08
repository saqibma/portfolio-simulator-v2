import csv
import os
import logging

from typing import List, Optional, Dict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Asset:
    def __init__(self, name: str, no_of_shares: int = 1, price: float = 0.0):
        self.name = name
        self.no_of_shares = no_of_shares
        self.price = price

    def __repr__(self) -> str:
        return (
            f"Asset(name='{self.name}', "
            f"no_of_shares={self.no_of_shares}, "
            f"price={self.price})"
        )


class PortfolioSimulator:
    def __init__(self):
        self.portfolios: Dict[Asset, Dict[str, Asset]] = {}
        self.child_parent_portfolio: Dict[str, Asset] = {}

    def load_portfolios(self, portfolio_file: str) -> None:
        with open(portfolio_file, "r") as file:
            reader = csv.reader(file)
            current_portfolio: Optional[Asset] = None
            for row in reader:
                if row[0] == "NAME":
                    continue
                elif row[0] and row[1]:
                    child_portfolios: Dict[str, Asset] = {}
                    if self.portfolios.get(current_portfolio):
                        child_portfolios = self.portfolios.get(current_portfolio)
                    child_portfolios[row[0]] = Asset(row[0], int(row[1]))
                    self.portfolios[current_portfolio] = child_portfolios
                    self.child_parent_portfolio[row[0]] = current_portfolio
                elif row[0] and not row[1]:
                    current_portfolio = Asset(row[0])

    def traverse_portfolios(self) -> None:
        logging.info(self.child_parent_portfolio)
        for parent_portfolio, assets in self.portfolios.items():
            logging.info(f"{parent_portfolio}:")
            for asset_name, asset in assets.items():
                logging.info(f"     {asset_name}: {asset}")

    def calculate_portfolio_prices(self, prices_file: str, output_file: str) -> None:
        with open(output_file, "w", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["NAME", "PRICE"])
            for chunk in self.stream_csv_in_chunks(prices_file, chunk_size=1):
                calculated_prices = []
                for row in chunk:
                    stock_name, stock_price = row[0], float(row[1])
                    writer.writerow([stock_name, stock_price])
                    self.update_asset_price(stock_name, stock_price, calculated_prices)
                writer.writerows(calculated_prices)

    def update_asset_price(
        self,
        stock_name: str,
        stock_price: float,
        calculated_prices: List[List],
    ) -> None:
        while self.child_parent_portfolio.get(stock_name):
            parent_portfolio: Asset = self.child_parent_portfolio[stock_name]
            if self.portfolios.get(parent_portfolio):
                child_portfolios: Dict[str, Asset] = self.portfolios.get(
                    parent_portfolio
                )
                child_portfolio: Asset = child_portfolios.get(stock_name)
                child_portfolio.price = stock_price
                if self.has_valid_prices(child_portfolios):
                    total_price = self.calculate_portfolio_value(child_portfolios)
                    calculated_prices.append([parent_portfolio.name, total_price])
                    stock_name = parent_portfolio.name
                    stock_price = total_price
                else:
                    return
            else:
                return

    @staticmethod
    def has_valid_prices(child_portfolios: Dict[str, Asset]) -> bool:
        """
        Check if all asset prices are non-zero
        """
        return len(child_portfolios) > 0 and all(
            asset.price != 0.0 for asset in child_portfolios.values()
        )

    @staticmethod
    def calculate_portfolio_value(child_portfolios: Dict[str, Asset]) -> float:
        """
        Calculate the total value of the portfolio by summing
        the product of price and number of shares for each asset
        """
        return sum(
            asset.price * asset.no_of_shares for asset in child_portfolios.values()
        )

    @staticmethod
    def stream_csv_in_chunks(filename: str, chunk_size: int = 10) -> List[List[str]]:
        with open(filename, "r") as file:
            reader = csv.reader(file)
            # Skip header if present
            next(reader, None)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:  # Yield any remaining records
                yield chunk


def main():
    portfolios_csv_path = os.path.join("..", "..", "data", "input", "portfolios.csv")
    prices_csv_path = os.path.join("..", "..", "data", "input", "prices.csv")
    portfolio_prices_csv_path = os.path.join(
        "..", "..", "data", "output", "portfolio_prices.csv"
    )
    simulator = PortfolioSimulator()
    simulator.load_portfolios(portfolios_csv_path)
    simulator.traverse_portfolios()
    simulator.calculate_portfolio_prices(prices_csv_path, portfolio_prices_csv_path)
    logging.info("********************************")
    simulator.traverse_portfolios()


if __name__ == "__main__":
    main()
