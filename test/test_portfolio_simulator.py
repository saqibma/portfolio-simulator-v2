from unittest.mock import patch, mock_open
import pytest
from src.portfolio_simulator.portfolio_simulator import PortfolioSimulator, Asset


def test_load_portfolios():
    # Mocking the open function to return test data
    with patch("builtins.open", mock_open(read_data="NAME,SHARES\nParent,\nChild1,100\nChild2,200\nParent2,\nChild3,300")):
        simulator = PortfolioSimulator()
        simulator.load_portfolios("mocked_portfolio.csv")
        print(simulator.portfolios)

        # Asserting the loaded portfolios and child-parent relationships
        assert str(simulator.portfolios) == str({
            Asset(name='Parent', no_of_shares=1, price=0.0): {
                'Child1': Asset(name='Child1', no_of_shares=100, price=0.0),
                'Child2': Asset(name='Child2', no_of_shares=200, price=0.0)
            },
            Asset(name='Parent2', no_of_shares=1, price=0.0): {
                'Child3': Asset(name='Child3', no_of_shares=300, price=0.0)
            }
        })

        assert str(simulator.child_parent_portfolio) == str({
            "Child1": Asset("Parent"),
            "Child2": Asset("Parent"),
            "Child3": Asset("Parent2")
        })


def test_stream_csv_in_chunks():
    # Mocking the open function to return test data
    with patch("builtins.open", mock_open(read_data="NAME,SHARES\nAAPL,150\nGOOGL,250\nMSFT,300\nTSLA,600\nAMZN,350")):
        simulator = PortfolioSimulator()
        chunks = list(simulator.stream_csv_in_chunks("mocked_prices.csv", chunk_size=1))

        # Asserting the streamed chunks
        assert chunks == [
            [["AAPL", "150"]],
            [["GOOGL", "250"]],
            [["MSFT", "300"]],
            [["TSLA", "600"]],
            [["AMZN", "350"]]
        ]


def test_calculate_portfolio_value():
    child_portfolios = {
        "AAPL": Asset("AAPL", price=150),
        "GOOGL": Asset("GOOGL", price=250),
        "MSFT": Asset("MSFT", price=300)
    }
    simulator = PortfolioSimulator()
    total_value = simulator.calculate_portfolio_value(child_portfolios)

    # Asserting the calculated portfolio value
    assert total_value == 150 * 1 + 250 * 1 + 300 * 1


@pytest.mark.parametrize("child_portfolios, expected_result", [
    ({"AAPL": Asset("AAPL", price=0)}, False),
    ({"AAPL": Asset("AAPL", price=150)}, True),
    ({"AAPL": Asset("AAPL", price=150),
      "GOOGL": Asset("GOOGL", price=250)}, True),
    ({"AAPL": Asset("AAPL", price=150),
      "GOOGL": Asset("GOOGL", price=0)}, False),
])
def test_has_valid_prices(child_portfolios, expected_result):
    simulator = PortfolioSimulator()
    result = simulator.has_valid_prices(child_portfolios)

    # Asserting the validity of prices
    assert result == expected_result

if __name__ == "__main__":
    pytest.main([__file__])

