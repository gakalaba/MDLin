import json
from mdlin import SyncAppRequest, InitCustom
import os
import json
import enum
import logging
import sys


class RankSortKeys(enum.Enum):
    ALL = "all"
    TOP10 = "top10"
    BOTTOM10 = "bottom10"


class RedisClient:
    def __init__(self):
        pass

    def set_init_data(self):
        print("Runnning set init data")
        with open(
            "/users/akalaba/basic-redis-leaderboard-demo-python-transformed/server/core/companies_data.json",
            "r",
        ) as init_data:
            companies = json.load(init_data)
            try:
                for company in companies:
                    symbol = self.add_prefix_to_symbol(
                        "redis", company.get("symbol").lower()
                    )
                    SyncAppRequest(
                        "ZADD",
                        "leaderboard",
                        {symbol: company.get("marketCap")},
                    )
                    SyncAppRequest("HSET", symbol, "company", company.get("company"))
                    SyncAppRequest("HSET", symbol, "country", company.get("country"))
            except:
                pass

    def add_prefix_to_symbol(self, prefix, symbol):
        return f"{prefix}:{symbol}"

    @staticmethod
    def remove_prefix_to_symbol(prefix, symbol):
        return symbol.replace(f"{prefix}:", "")


class CompaniesRanks(RedisClient):
    def update_company_market_capitalization(self, amount, symbol):
        SyncAppRequest(
            "ZINCRBY",
            "leaderboard",
            amount,
            self.add_prefix_to_symbol("redis", symbol),
        )
        return None

    def get_ranks_by_sort_key(self, key):
        sort_key = RankSortKeys(key)
        if sort_key is RankSortKeys.ALL:
            return self.get_zrange(0, -1)
        elif sort_key is RankSortKeys.TOP10:
            return self.get_zrange(0, 9)
        elif sort_key is RankSortKeys.BOTTOM10:
            return self.get_zrange(0, 9, False)

    def get_ranks_by_symbols(self, symbols):
        companies_capitalization = []
        for symbol in symbols:
            companies_capitalization.append(
                SyncAppRequest(
                    "ZSCORE",
                    "leaderboard",
                    self.add_prefix_to_symbol("redis", symbol),
                )
            )
        print("Companies captailziations", companies_capitalization)
        companies = []
        for index, market_capitalization in enumerate(companies_capitalization):
            companies.append(
                self.add_prefix_to_symbol("redis", symbols[index]),
            )
        print("Companies result ", companies)
        return self.get_result(companies)

    def get_zrange(self, start_index, stop_index, desc=True):
        print("Calling get zrange")
        query_args = {
            "name": "leaderboard",
            "start": start_index,
            "end": stop_index,
            "withscores": True,
            "score_cast_func": str,
        }
        if desc:
            companies = SyncAppRequest(
                "ZREVRANGE", "leaderboard", start_index, stop_index
            )
        else:
            print("Sending zrange")
            companies = SyncAppRequest("ZRANGE", "leaderboard", start_index, stop_index)
        print("Compelted get zrange", companies)
        return self.get_result(companies, start_index, desc)

    def get_result(self, companies, start_index=0, desc=True):
        start_rank = int(start_index) + 1 if desc else len(companies) - start_index
        increase_factor = 1 if desc else -1
        results = []
        for company in companies:
            symbol = company
            # market_cap = company[1]
            print("Input to HGET all", symbol)
            company_info = SyncAppRequest("HGETALL", company)
            print("Reuslt company info", company_info)
            print("Reuslt company info type", type(company_info))
            results.append(
                {
                    "company": company_info["company"],
                    "country": company_info["country"],
                    "marketCap": 100,
                    "rank": start_rank,
                    "symbol": self.remove_prefix_to_symbol("redis", symbol),
                }
            )
            start_rank += increase_factor
        return json.dumps(results)


if __name__ == "__main__":
    # Initialize custom configurations or settings
    InitCustom("0", "multi_paxos")

    # Test RedisClient and CompaniesRanks
    redis_client = RedisClient()

    # Test setting initial data from the companies_data.json file
    print("Running set_init_data...")
    redis_client.set_init_data()

    # # Test updating a company's market capitalization
    # print("Updating market capitalization for 'AAPL'...")
    companies_ranks = CompaniesRanks()
    # companies_ranks.update_company_market_capitalization(5000000, "AAPL")

    # # Test getting ranks by sort key (e.g., TOP10)
    # print("Fetching TOP10 ranks...")
    # top_ranks = companies_ranks.get_ranks_by_sort_key(RankSortKeys.TOP10.value)
    # print("Top Ranks:", top_ranks)

    # Test getting ranks by specific symbols
    print("Fetching ranks for specific symbols ['AAPL', 'GOOG']...")
    ranks = companies_ranks.get_ranks_by_symbols(["AAPL", "GOOG"])
    print("Ranks by Symbols:", ranks)
