"""Simulated stock and cryptocurrency publishers for the demo."""
from __future__ import annotations

import argparse
import random
import time
from typing import Dict, Any

from common import protocol as P
from publisher.client import PublisherClient


class RandomWalkFeed:
    """Generate small random price changes and publish each asset update."""

    def __init__(
        self,
        publisher: PublisherClient,
        assets: Dict[str, Dict[str, Any]],
        interval: float = 2.0,
        max_change_pct: float = 2.0,
    ) -> None:
        self.publisher = publisher
        self.assets = assets
        self.interval = interval
        self.max_change_pct = max_change_pct

    def next_price(self, current_price: float) -> tuple[float, float]:
        """Return (new_price, percentage_change) for one random-walk step."""
        change_pct = random.uniform(
            -self.max_change_pct, self.max_change_pct
        )
        new_price = max(0.01, current_price * (1 + change_pct / 100))
        return round(new_price, 2), round(change_pct, 4)

    def run(self) -> None:
        if not self.publisher.connect():
            raise ConnectionError(
                f"Could not connect publisher {self.publisher.client_id}"
            )

        print(
            f"\n[FEED STARTED] {self.publisher.client_id} "
            f"(interval={self.interval}s)"
        )

        try:
            while True:
                for symbol, asset in self.assets.items():
                    previous_price = asset["price"]
                    new_price, change_pct = self.next_price(previous_price)
                    asset["price"] = new_price

                    payload = {
                        "symbol": symbol,
                        "price": new_price,
                        "change_pct": change_pct,
                    }

                    topic = asset["topic"]
                    print(
                        f"[{topic}] {symbol}: "
                        f"${previous_price:,.2f} -> ${new_price:,.2f} "
                        f"({change_pct:+.2f}%)"
                    )

                    published = self.publisher.publish(topic, payload)
                    if not published:
                        print(
                            f"[WARNING] Could not publish {symbol}; "
                            "the next update will try again."
                        )

                time.sleep(self.interval)

        except KeyboardInterrupt:
            print("\n[FEED STOPPED] Keyboard interrupt received.")
        finally:
            self.publisher.disconnect()


def stock_assets() -> Dict[str, Dict[str, Any]]:
    """Publisher A: technology and banking stock topics."""
    return {
        "AAPL": {"topic": "stocks.tech", "price": 225.50},
        "MSFT": {"topic": "stocks.tech", "price": 415.00},
        "NVDA": {"topic": "stocks.tech", "price": 180.25},
        "JPM": {"topic": "stocks.banking", "price": 250.00},
        "BAC": {"topic": "stocks.banking", "price": 45.50},
    }


def crypto_assets() -> Dict[str, Dict[str, Any]]:
    """Publisher B: cryptocurrency topic."""
    return {
        "BTC": {"topic": "crypto", "price": 95000.00},
        "ETH": {"topic": "crypto", "price": 3500.00},
        "SOL": {"topic": "crypto", "price": 180.00},
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the stock or cryptocurrency price feed."
    )
    parser.add_argument(
        "--feed",
        choices=("stocks", "crypto"),
        required=True,
        help="Which publisher feed to run.",
    )
    parser.add_argument("--host", default=P.DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=P.DEFAULT_PORT)
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds to wait between publishing batches.",
    )
    parser.add_argument(
        "--max-change-pct",
        type=float,
        default=2.0,
        help="Maximum absolute random price movement per update (percent).",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.max_change_pct < 0 or args.max_change_pct >= 100:
        parser.error("--max-change-pct must be between 0 and 100")

    if args.feed == "stocks":
        client_id = "stock-exchange"
        assets = stock_assets()
    else:
        client_id = "crypto-feed"
        assets = crypto_assets()

    publisher = PublisherClient(
        client_id=client_id,
        host=args.host,
        port=args.port,
    )
    feed = RandomWalkFeed(
        publisher=publisher,
        assets=assets,
        interval=args.interval,
        max_change_pct=args.max_change_pct,
    )
    feed.run()


if __name__ == "__main__":
    main()
