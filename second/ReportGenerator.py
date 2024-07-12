import os, time, asyncio
import polars as pl
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
class FundReportGenerator:
    def __init__(self):
        self.fund_report_path = ""
        self.equity_report_path = ""
        self.bond_report_path = ""
        self.time_now = time.strftime("%Y_%m_%d_%H%M%S")
        self.reports_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'reports'))
        if not os.path.exists(self.reports_path):
            os.mkdir(self.reports_path)
        self.price_reco_path = os.path.join(self.reports_path, "[FUNDS]PRICE_RECO_{}".format(self.time_now))
        if not os.path.exists(self.price_reco_path):
            os.mkdir(self.price_reco_path)
        self.db_uri = os.getenv("DB_URL")
        self.engine = create_engine(self.db_uri)

        # Maybe DRY too much
        self.external_funds_query = os.getenv("EXTERNAL_FUNDS")
        self.equities_prices_query = os.getenv("EQUITIES_PRICES")
        self.bonds_prices_query = os.getenv("BONDS_PRICES")
        self.monthly_performing_view = os.getenv("MONTHLY_PERFORMING")
        self.all_time_performing_view = os.getenv("ALL_TIME_PERFORMING")
        self.external_funds = pl.read_database(query=self.external_funds_query, connection=self.engine.connect(),
                                               infer_schema_length=None)
        self.equity_prices = pl.read_database(query=self.equities_prices_query, connection=self.engine.connect(),
                                              infer_schema_length=None)
        self.bond_prices = pl.read_database(query=self.bonds_prices_query, connection=self.engine.connect(),
                                            infer_schema_length=None)
        self.funds = pl.Series(self.external_funds.select(pl.col("FUND").unique())).sort().to_list()

    async def main(self):
        try:
            self.logger("RUNNING PRICE RECONCILIATION REPORT GENERATOR")
            start_time = time.time()
            tasks = []
            for fund in self.funds:
                task = asyncio.create_task(self.generate_report(fund=fund))
                tasks.append(task)

            check_all = [await task for task in tasks]
            if all(check_all):
                self.logger("Completed Price Reconciliation Report Generation")
            end_time = time.time()

            self.logger(msg="Generating Views for Monthly & All Time Top Performing Fund")
            top_fund_monthly = self.get_view(query=self.monthly_performing_view)
            top_fund_max = self.get_view(query=self.all_time_performing_view)
            pl.Config().set_tbl_rows(13)
            pl.Config.set_tbl_hide_dataframe_shape(True)
            pl.Config.set_tbl_hide_column_data_types(True)
            self.logger(msg="Monthly Top Performing Funds\n{}".format(top_fund_monthly))
            self.logger(msg="All Time Top Performing Fund\n{}".format(top_fund_max))
            self.logger("Total Execution Time: {}".format(end_time - start_time))
        except Exception as err:
            self.logger(msg=err)

    async def process_data(self, fund: str, name: str, path: str, equity: bool, positions: pl.dataframe,
                           prices: pl.dataframe):
        try:
            uni_col = "SYMBOL" if equity else "ISIN"
            date_format = "%m/%d/%Y" if equity else "%Y-%m-%d"
            instrument_prices = prices.filter(pl.col(uni_col) == name)

            # Add columns with Month-Year
            get_month_year_df = instrument_prices.select(
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_format, strict=False),
                pl.col('PRICE'),
                pl.col(uni_col),
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_format, strict=False).dt.month().alias('MONTH'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_format, strict=False).dt.year().alias('YEAR')
            ).sort('DATETIME')

            # Partition data by Month, Year and SYMBOL/ISIN
            month_year_partition_df = get_month_year_df.partition_by('YEAR', 'MONTH', uni_col)

            # Get all the end of month price data from master reference by Month, Year
            month_year_df = None
            for partition in month_year_partition_df:
                # Extending data frame one by one
                temp_df = pl.DataFrame(partition)
                eom_date = temp_df.select(pl.last("DATETIME", "PRICE", uni_col, "MONTH", "YEAR"))
                if month_year_df is None:
                    month_year_df = eom_date
                else:
                    month_year_df.extend(eom_date)

            # Price data from master reference
            filtered_month_year_df = month_year_df.select(pl.col(uni_col), pl.col('DATETIME').sort(),
                                                          pl.col('PRICE').alias('REF PRICE'), pl.col('MONTH'),
                                                          pl.col('YEAR'))

            # Price data from reference row that is matching to fund's report date(month, year)
            filtered_fund_df = (positions.filter(pl.col("SYMBOL") == name).sort(by=pl.col('DATE')).select(
                pl.all(),
                pl.col("DATE").dt.month().alias('MONTH'),
                pl.col("DATE").dt.year().alias('YEAR')))

            # Join the two tables and will have ref price and fund price side by side
            join_price_df = filtered_month_year_df.join(filtered_fund_df, on=["YEAR", "MONTH"], how="inner")

            # Generate result df for writing to csv
            result_df = join_price_df.select(pl.col(uni_col), pl.col("REF PRICE"),
                                             pl.col("DATETIME").alias("REF DATE"),
                                             pl.col("PRICE").alias("FUND PRICE"),
                                             pl.col("DATE").alias("FUND DATE"),
                                             (pl.col("REF PRICE").cast(pl.Float64) - pl.col("PRICE").cast(pl.Float64))
                                             .alias("PRICE DIFF"))

            report_name = os.path.join(path, "[{}]{}_{}_{}.csv".format("Equity" if equity else "Bond", fund, name,
                                                                       self.time_now))

            result_df.write_csv(report_name, datetime_format="%Y-%m-%d")

            return True if os.path.exists(report_name) else False
        except Exception as err:
            self.logger(msg=err)

    async def generate_report(self, fund: str):
        try:
            self.logger(msg="Generating ({}) Monthly Price Reconciliation Report".format(fund.upper()))

            self.initialize_saves(fund=fund)

            fund_positions = self.external_funds.filter(pl.col('FUND') == fund)

            result_equities = [
                asyncio.create_task(self.process_data(fund=fund, name=symbol, path=self.equity_report_path,
                                                      equity=True, positions=fund_positions,
                                                      prices=self.equity_prices))
                for symbol in self.get_instruments(positions=fund_positions, name="Equities")
            ]

            result_bonds = [
                asyncio.create_task(self.process_data(fund=fund, name=bond, path=self.bond_report_path,
                                                      equity=False, positions=fund_positions,
                                                      prices=self.bond_prices))
                for bond in self.get_instruments(positions=fund_positions, name="Government Bond")
            ]

            return True if all([await task for task in [*result_equities, *result_bonds]]) else False

        except Exception as err:
            self.logger(msg=err)

    def get_view(self, query: str):
        try:
            return pl.read_database(query, connection=self.engine.connect(), infer_schema_length=None)
        except Exception as err:
            self.logger(msg=err)

    def initialize_saves(self, fund: str):
        try:
            created, paths = [], []
            self.fund_report_path = os.path.join(self.price_reco_path, fund)
            self.equity_report_path = os.path.join(self.fund_report_path, 'Equities')
            self.bond_report_path = os.path.join(self.fund_report_path, 'Bonds')
            paths.extend((self.fund_report_path, self.equity_report_path, self.bond_report_path))

            if len(paths) == 1:
                os.mkdir(paths[0])
                return True if os.path.exists(paths[0]) else False
            else:
                for path in paths:
                    os.mkdir(path)
                    if os.path.exists(path):
                        created.append(True)
            return True if all(created) else False
        except Exception as err:
            self.logger(msg=err)

    def logger(self, msg):
        return print("{} - {}".format(str(datetime.now()), msg))

    def get_instruments(self, positions: pl.dataframe, name: str):
        try:
            return pl.Series(positions.filter(pl.col("FINANCIAL TYPE") == name)["SYMBOL"]).unique().sort().to_list()
        except Exception as err:
            self.logger(msg=err)
if __name__ == '__main__':
    reports = FundReportGenerator()
    asyncio.run(reports.main())
