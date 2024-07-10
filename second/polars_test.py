import asyncio
import os
from pathlib import Path

import polars as pl
from sqlalchemy import create_engine
import time

class GenerateFundReport:
    def __init__(self):
        [f.unlink() for f in Path(r"C:\Users\PC\Desktop\Git\gic_assessment\reports").glob("*") if f.is_file()]
        self.time_now = time.strftime("%Y_%m_%d_%H%M%S")
        self.report_path = r"C:\Users\PC\Desktop\Git\gic_assessment\reports\REPORT_{}".format(self.time_now)
        os.mkdir(self.report_path)
        self.db_uri = r'postgresql://postgres:admin123@localhost:5432/gic_funds'
        self.engine = create_engine(self.db_uri)
        self.external_funds_query = 'SELECT * FROM external_funds'
        self.equities_prices_query = 'SELECT * FROM equity_prices'
        self.bonds_prices_query = 'SELECT * FROM bond_prices'

        self.external_funds = pl.read_database(query=self.external_funds_query, connection=self.engine.connect(),
                                          infer_schema_length=None)
        self.equity_prices = pl.read_database(query=self.equities_prices_query, connection=self.engine.connect(),
                                         infer_schema_length=None)
        self.bond_prices = pl.read_database(query=self.bonds_prices_query, connection=self.engine.connect(), infer_schema_length=None)
        self.funds = pl.Series(self.external_funds.select(pl.col("FUND").unique())).sort().to_list()

    def normal_generator(self):
        start_time = time.time()
        for fund in self.funds:
            print(fund)
            fund_positions = self.external_funds.filter(pl.col('FUND') == fund)
            symbols = pl.Series(
                fund_positions.filter(pl.col('FINANCIAL TYPE') == 'Equities')['SYMBOL']).unique().sort().to_list()
            for symbol in symbols:
                equity_price = self.equity_prices.filter(pl.col('SYMBOL') == symbol)
                get_closes_day_to_eom = equity_price.select(
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False), pl.col('PRICE'),
                    pl.col('SYMBOL'),
                    (pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y",
                                                     strict=False).dt.month_end().dt.day() - pl.col('DATETIME').str.strptime(
                        pl.Datetime, format="%m/%d/%Y", strict=False).dt.day()).alias('DAY TO EOM'),
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False).dt.month().alias('MONTH'),
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False).dt.year().alias('YEAR')).sort(
                    'DATETIME')
                # print(get_closes_day_to_eom.head())
                partition_by_year_month = get_closes_day_to_eom.partition_by('YEAR', 'MONTH', 'SYMBOL')
                df_overall = None
                for partition in partition_by_year_month:
                    temp_df = pl.DataFrame(partition)
                    eom_date = temp_df.select(pl.last("DATETIME", "PRICE", "SYMBOL", "MONTH", "YEAR"))

                    if df_overall is None:
                        df_overall = eom_date
                    else:
                        df_overall.extend(eom_date)

                master_ref_by_symbol = df_overall.select(pl.col('SYMBOL'), pl.col('DATETIME').sort(),
                                                         pl.col('PRICE').alias('REF PRICE'), pl.col('MONTH'), pl.col('YEAR'))
                fund_ref_by_symbol = (fund_positions.filter(pl.col('SYMBOL') == symbol).sort(by=pl.col('DATE'))
                                      .select(pl.all(),
                                              pl.col(
                                                  "DATE").dt.month().alias(
                                                  'MONTH'),
                                              pl.col(
                                                  "DATE").dt.year().alias(
                                                  'YEAR')))
                price_ref_by_symbol = master_ref_by_symbol.join(fund_ref_by_symbol, on=["YEAR", "MONTH"], how="inner")
                print(price_ref_by_symbol.select(pl.col("SYMBOL"), pl.col("REF PRICE"), pl.col("DATETIME").alias("REF DATE"),
                                                 pl.col("PRICE").alias("FUND PRICE"), pl.col("DATE").alias("FUND DATE"), (
                                                         pl.col("REF PRICE").cast(pl.Float64) - pl.col("PRICE").cast(
                                                     pl.Float64)).alias("PRICE DIFF")))


            bonds = pl.Series(
                fund_positions.filter(pl.col('FINANCIAL TYPE') == 'Government Bond')['SYMBOL']).unique().sort().to_list()
            for bond in bonds:
                bond_price = self.bond_prices.filter(pl.col('ISIN') == bond)
                get_closes_day_to_eom = bond_price.select(
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False), pl.col('PRICE'),
                    pl.col('ISIN'),
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).dt.month().alias('MONTH'),
                    pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).dt.year().alias('YEAR')).sort(
                    'DATETIME')
                partition_by_year_month = get_closes_day_to_eom.partition_by('YEAR', 'MONTH', 'ISIN')
                df_overall = None
                for partition in partition_by_year_month:
                    temp_df = pl.DataFrame(partition)
                    # Get last row of the partition
                    eom_date = temp_df.select(pl.last("DATETIME", "PRICE", "ISIN", "MONTH", "YEAR"))

                    if df_overall is None:
                        df_overall = eom_date
                    else:
                        df_overall.extend(eom_date)

                master_ref_by_bond = df_overall.select(pl.col('ISIN'), pl.col('DATETIME').sort(),
                                                       pl.col('PRICE').alias('REF PRICE'), pl.col('MONTH'), pl.col('YEAR'))
                fund_ref_by_bond = (fund_positions.filter(pl.col('SYMBOL') == bond).sort(by=pl.col('DATE'))
                                    .select(pl.all(),
                                            pl.col(
                                                "DATE").dt.month().alias(
                                                'MONTH'),
                                            pl.col(
                                                "DATE").dt.year().alias(
                                                'YEAR')))
                price_ref_by_symbol = master_ref_by_bond.join(fund_ref_by_bond, on=["YEAR", "MONTH"], how="inner")
                print(price_ref_by_symbol.select(pl.col("SYMBOL"), pl.col("REF PRICE"), pl.col("DATETIME").alias("REF DATE"),
                                                 pl.col("PRICE").alias("FUND PRICE"), pl.col("DATE").alias("FUND DATE"), (
                                                         pl.col("REF PRICE").cast(pl.Float64) - pl.col("PRICE").cast(
                                                     pl.Float64)).alias("PRICE DIFF")))
        end_time = time.time()
        print("Total Execution Time: {}".format(end_time-start_time))

    async def main(self):
        tasks = []
        start_time = time.time()
        for fund in self.funds:
            task = asyncio.create_task(self.generate_report(fund=fund))
            tasks.append(task)

        check_all = [await task for task in tasks]
        print(check_all)
        end_time = time.time()
        print("Total Execution Time: {}".format(end_time - start_time))
        if all(check_all):
            print("Completed Report Generation")


    async def generate_report(self, fund):
        print("Generating report for {}".format(fund.upper()))
        self.fund_report_path = os.path.join(self.report_path, fund)
        os.mkdir(os.path.join(self.report_path, fund))
        self.equity_report_path = os.path.join(self.fund_report_path, 'Equities')
        os.mkdir(self.equity_report_path)
        self.bond_report_path = os.path.join(self.fund_report_path, 'Bonds')
        os.mkdir(self.bond_report_path)
        fund_positions = self.external_funds.filter(pl.col('FUND') == fund)
        symbols = pl.Series(
            fund_positions.filter(pl.col('FINANCIAL TYPE') == 'Equities')['SYMBOL']).unique().sort().to_list()
        for symbol in symbols:
            equity_price = self.equity_prices.filter(pl.col('SYMBOL') == symbol)
            get_closes_day_to_eom = equity_price.select(
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False), pl.col('PRICE'),
                pl.col('SYMBOL'),
                (pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y",
                                                 strict=False).dt.month_end().dt.day() - pl.col(
                    'DATETIME').str.strptime(
                    pl.Datetime, format="%m/%d/%Y", strict=False).dt.day()).alias('DAY TO EOM'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False).dt.month().alias('MONTH'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False).dt.year().alias(
                    'YEAR')).sort(
                'DATETIME')
            # print(get_closes_day_to_eom.head())
            partition_by_year_month = get_closes_day_to_eom.partition_by('YEAR', 'MONTH', 'SYMBOL')
            df_overall = None
            for partition in partition_by_year_month:
                temp_df = pl.DataFrame(partition)
                eom_date = temp_df.select(pl.last("DATETIME", "PRICE", "SYMBOL", "MONTH", "YEAR"))

                if df_overall is None:
                    df_overall = eom_date
                else:
                    df_overall.extend(eom_date)

            master_ref_by_symbol = df_overall.select(pl.col('SYMBOL'), pl.col('DATETIME').sort(),
                                                     pl.col('PRICE').alias('REF PRICE'), pl.col('MONTH'),
                                                     pl.col('YEAR'))
            fund_ref_by_symbol = (fund_positions.filter(pl.col('SYMBOL') == symbol).sort(by=pl.col('DATE'))
                                  .select(pl.all(),
                                          pl.col(
                                              "DATE").dt.month().alias(
                                              'MONTH'),
                                          pl.col(
                                              "DATE").dt.year().alias(
                                              'YEAR')))
            price_ref_by_symbol = master_ref_by_symbol.join(fund_ref_by_symbol, on=["YEAR", "MONTH"], how="inner")
            to_write_equity = price_ref_by_symbol.select(pl.col("SYMBOL"), pl.col("REF PRICE"), pl.col("DATETIME").alias("REF DATE"),
                                           pl.col("PRICE").alias("FUND PRICE"), pl.col("DATE").alias("FUND DATE"), (
                                                   pl.col("REF PRICE").cast(pl.Float64) - pl.col("PRICE").cast(
                                               pl.Float64)).alias("PRICE DIFF"))

            to_write_equity.write_csv(os.path.join(self.equity_report_path, "[{}]{}_{}_{}.csv".format("Equity", fund, symbol, self.time_now)), datetime_format="%Y-%m-%d")


        bonds = pl.Series(
            fund_positions.filter(pl.col('FINANCIAL TYPE') == 'Government Bond')['SYMBOL']).unique().sort().to_list()
        for bond in bonds:
            bond_price = self.bond_prices.filter(pl.col('ISIN') == bond)
            get_closes_day_to_eom = bond_price.select(
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False), pl.col('PRICE'),
                pl.col('ISIN'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).dt.month().alias('MONTH'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).dt.year().alias(
                    'YEAR')).sort(
                'DATETIME')
            partition_by_year_month = get_closes_day_to_eom.partition_by('YEAR', 'MONTH', 'ISIN')
            df_overall = None
            for partition in partition_by_year_month:
                temp_df = pl.DataFrame(partition)
                # Get last row of the partition
                eom_date = temp_df.select(pl.last("DATETIME", "PRICE", "ISIN", "MONTH", "YEAR"))

                if df_overall is None:
                    df_overall = eom_date
                else:
                    df_overall.extend(eom_date)

            master_ref_by_bond = df_overall.select(pl.col('ISIN'), pl.col('DATETIME').sort(),
                                                   pl.col('PRICE').alias('REF PRICE'), pl.col('MONTH'), pl.col('YEAR'))
            fund_ref_by_bond = (fund_positions.filter(pl.col('SYMBOL') == bond).sort(by=pl.col('DATE'))
                                .select(pl.all(),
                                        pl.col(
                                            "DATE").dt.month().alias(
                                            'MONTH'),
                                        pl.col(
                                            "DATE").dt.year().alias(
                                            'YEAR')))
            price_ref_by_symbol = master_ref_by_bond.join(fund_ref_by_bond, on=["YEAR", "MONTH"], how="inner")
            to_write_bond = price_ref_by_symbol.select(pl.col("SYMBOL"), pl.col("REF PRICE"), pl.col("DATETIME").alias("REF DATE"),
                                           pl.col("PRICE").alias("FUND PRICE"), pl.col("DATE").alias("FUND DATE"), (
                                                   pl.col("REF PRICE").cast(pl.Float64) - pl.col("PRICE").cast(
                                               pl.Float64)).alias("PRICE DIFF"))

            to_write_bond.write_csv(
                os.path.join(self.bond_report_path, "[{}]{}_{}_{}.csv".format("Bond", fund, bond, self.time_now)), datetime_format="%Y-%m-%d")

        return True

if __name__ == '__main__':
    get_reports = GenerateFundReport()
    asyncio.run(get_reports.main())