import os
import time
import asyncio
import polars as pl
from datetime import datetime
from dotenv import load_dotenv
from xlsxwriter import Workbook
from sqlalchemy import create_engine

load_dotenv()
db_uri = os.getenv("DB_URL")
engine = create_engine(db_uri)
conn = engine.connect()


class FundReportGenerator:
    def __init__(self):
        self.__params()
        self.__general_save_file()
        self.__polars_config()
        self.__db_get_master()

    # Initialize parameters
    def __params(self):
        try:
            self.time_now = time.strftime("%Y_%m_%d_%H%M%S")
            self.fund_report_path = ""
            self.equity_report_path = ""
            self.bond_report_path = ""
            self.write_to_db = os.getenv("WRITE_TO_DB")
        except Exception as err:
            self.logger(msg=err)

    # Create external folder for save files
    def __general_save_file(self):
        try:
            self.paths = {
                "reports": os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'reports'))
            }
            if not os.path.exists(self.paths["reports"]):
                os.mkdir(self.paths["reports"])
            self.price_reco_path = os.path.join(self.paths["reports"], "[FUNDS]PRICE_RECO_{}".format(self.time_now))
            if not os.path.exists(self.price_reco_path):
                os.mkdir(self.price_reco_path)
        except Exception as err:
            self.logger(msg=err)

    # Initialize Polars configuration
    def __polars_config(self):
        try:
            pl.Config.set_tbl_hide_dataframe_shape(True)
            pl.Config.set_tbl_hide_column_data_types(True)
            pl.Config().set_tbl_rows(20)
        except Exception as err:
            self.logger(msg=err)

    # Initialize database
    def __db_get_master(self):
        try:
            # Need to shorten this part & with psycopg2 class
            self.queries = {
                "external_funds": os.getenv("EXTERNAL_FUNDS"),
                "funds_report": os.getenv("FUNDS_REPORT"),
                "equities_prices": os.getenv("EQUITIES_PRICES"),
                "bonds_prices": os.getenv("BONDS_PRICES"),
                "monthly_performing": os.getenv("MONTHLY_PERFORMING"),
                "all_time_performing": os.getenv("ALL_TIME_PERFORMING")
            }
            # self.__get_master_reference(queries=self.queries)
            # need func
            self.funds_report = pl.read_database(query=self.queries["funds_report"], connection=conn,
                                                 infer_schema_length=None)
            self.equity_prices = pl.read_database(query=self.queries["equities_prices"], connection=conn,
                                                  infer_schema_length=None)
            self.bond_prices = pl.read_database(query=self.queries["bonds_prices"], connection=conn,
                                                infer_schema_length=None)
            self.funds = pl.Series(self.funds_report.select(pl.col("fund").unique())).sort().to_list()
        except Exception as err:
            self.logger(msg=err)

    async def __get_master_reference(self, queries: dict):
        try:
            master_reference = dict()
            for key, query in queries.items():
                res = pl.read_database(query=query, connection=conn, infer_schema_length=None)
                if res.is_empty():
                    raise(f"Master reference data for {key} is empty.")
                if "funds_report" == key:
                    master_reference["funds"] = pl.Series(res.select(pl.col("fund").unique())).sort().to_list()
                master_refence[key] = res
            # asyncio approach to be considered
            return master_reference
        except Exception as err:
            self.logger(msg=err)

    async def main(self):
        """
        Main function to run Price Reconciliation Report Generator
        """
        try:
            # Start of report generation
            self.logger("Running Price Reconciliation Report Generator")
            start_time = time.time()
            tasks = []

            for fund in self.funds:
                task = asyncio.create_task(self.generate_report(fund=fund))
                tasks.append(task)
            check_all = [await task for task in tasks]

            self.logger(f"Price Reconciliation Report Generation: {'Complete' if all(check_all) else 'Incomplete'}")

            # Get views
            self.logger(msg="Generating Views for Monthly & All Time Top Performing Fund")
            top_fund_monthly = self.get_view(query=self.queries["monthly_performing"])
            top_fund_max = self.get_view(query=self.queries["all_time_performing"])
            self.logger(msg=f"Monthly Top Performing Funds\n{ top_fund_monthly }")
            self.logger(msg=f"All Time Top Performing Fund\n{ top_fund_max }")
            end_time = time.time()
            self.logger(f"Total Execution Time: { end_time - start_time }")
        except Exception as err:
            self.logger(msg=err)
        finally:
            conn.close()

    async def generate_report(self, fund: str):
        try:

            self.create_save_files(fund=fund)

            self.logger(msg=f"Generating ({fund.upper()}) Monthly Price Reconciliation Report at {self.fund_report_path}")

            # Filter data from funds_report df
            fund_positions = self.funds_report.filter(pl.col('fund') == fund)

            # Loop to create async task to process equity data
            result_equities = [
                asyncio.create_task(self.process_data(fund=fund, instr_name=symbol, path=self.equity_report_path,
                                                      equity=True, positions=fund_positions,
                                                      ref_prices=self.equity_prices))
                for symbol in self.get_instruments(positions=fund_positions, name="Equities")
            ]

            # Loop to create async task to process bond data
            result_bonds = [
                asyncio.create_task(self.process_data(fund=fund, instr_name=bond, path=self.bond_report_path,
                                                      equity=False, positions=fund_positions,
                                                      ref_prices=self.bond_prices))
                for bond in self.get_instruments(positions=fund_positions, name="Government Bond")
            ]

            return all([await task for task in [*result_equities, *result_bonds]])
        except Exception as err:
            self.logger(msg=err)

    async def process_data(self, fund: str, instr_name: str, path: str, equity: bool, positions: pl.dataframe,
                           ref_prices: pl.dataframe):
        try:
            processes = []
            instr_key = "SYMBOL" if equity else "ISIN"  # For different column name usage
            date_format = "%m/%d/%Y" if equity else "%Y-%m-%d"  # For different table date format

            processes.append(asyncio.create_task(
                self.process_reference_data(data=ref_prices, name=instr_name, instr_key=instr_key,
                                            date_fmt=date_format)))
            processes.append(asyncio.create_task(self.process_report_data(positions=positions, name=instr_name)))

            check = [await x for x in processes]  # list of DataFrame

            joined_data = self.join_report_reference(data=check)

            filter_joined = self.select_preferred(data=joined_data, fund=fund, instr_key=instr_key, check_equity=equity)

            check_excel = self.write_to_excel(data=filter_joined, instr_key=instr_key, path=path, check_equity=equity,
                                              fund=fund, instr_name=instr_name)

            return check_excel
        except Exception as err:
            self.logger(msg=err)

    def select_preferred(self, data: pl.DataFrame, fund: str, instr_key: str, check_equity: bool):
        last_updated = datetime.strptime(self.time_now, "%Y_%m_%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        result_df = (
            data.with_columns(
                pl.lit(fund).alias("fund"),
                pl.lit(last_updated).alias("last_updated"),
                pl.lit("Equities" if check_equity else "Government Bond").alias("security_type")
            ).select(
                pl.col("fund"),
                pl.col("security_type"),
                pl.col("security_name"),
                pl.col(instr_key.lower()).alias("instrument_id"),
                pl.col("ref_price"),
                pl.col("ref_datetime").dt.strftime("%Y-%b-%d").alias("ref_datetime"),
                pl.col("price").alias("price"),
                pl.col("datetime").dt.strftime("%Y-%b-%d").alias("datetime"),
                (pl.col("price").cast(pl.Float64) - pl.col("ref_price").cast(pl.Float64)).alias("price_diff"),
                pl.col("last_updated")
            ))

        if "TRUE" in self.write_to_db:
            result_df.write_database("price_difference", connection=conn, if_table_exists="append")

        return result_df

    def write_to_excel(self, data: pl.DataFrame, instr_key: str, path: str, check_equity: bool, fund: str, instr_name: str):
        """
        Write to excel .xlsx with price difference data
        """
        try:
            # Specifying workbook location for save
            workbook_loc = os.path.join(path, "[{}]{}_{}_{}.xlsx".format("Equity" if check_equity else "Bond", fund,
                                                                       instr_name, self.time_now))

            # Extracting security name for each instrument
            security_name = data.select(pl.col("security_name").first()).item()

            # Write to excel
            with Workbook(workbook_loc) as wb:
                data.write_excel(
                    workbook=wb,
                    worksheet=instr_name,
                    position=(4, 1),
                    table_style="Table Style Light 16",
                    autofit=True,
                )
                ws = wb.get_worksheet_by_name(instr_name)
                fmt_title = wb.add_format(
                    {
                        "font_color": "#000000",
                        "font_size": 13,
                        "bold": True,
                    }
                )
                ws.write(1, 1, f"Name: {security_name} ({'Govt. Bond' if instr_key == 'ISIN' else 'Equity'})", fmt_title)
                ws.write(2, 1, f"Date: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}", fmt_title)

            return True if os.path.exists(workbook_loc) else False
        except Exception as err:
            self.logger(msg=err)

    def join_report_reference(self, data: list) -> pl.DataFrame:
        """
        Inner joining dataframes (reference & report) by month, year
        """
        try:
            return data[0].join(data[1], on=["year", "month"], how="inner")
        except Exception as err:
            self.logger(msg=err)

    async def process_report_data(self, positions: pl.DataFrame, name: str) -> pl.DataFrame:
        """
        Process funds report (external funds) data by filtering instrument name, month, year
        """
        try:
            # print(f"Running process_report_data for {name}")
            filtered_fund_df = (
                    positions.filter(
                        pl.col("symbol") == name
                    )
                    .sort(by=pl.col('datetime'))
                    .select(
                        pl.all(),
                        pl.col("datetime").dt.month().alias('month'),
                        pl.col("datetime").dt.year().alias('year')
                    )
                )
            return filtered_fund_df
        except Exception as err:
            self.logger(msg=err)

    async def process_reference_data(self, data: pl.DataFrame, name: str, instr_key: str, date_fmt: str) -> pl.DataFrame:
        """
        Process master reference data to extract price value by month, year
        """
        try:
            # Add columns with Month-Year
            instr_key_lowered = instr_key.lower()
            ref_prices = data.filter(pl.col(instr_key) == name)

            get_month_year_df = ref_prices.select(
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_fmt, strict=False).alias("ref_datetime"),
                pl.col('PRICE').alias("price"),
                pl.col(instr_key).alias(instr_key_lowered),
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_fmt, strict=False).dt.month().alias('month'),
                pl.col('DATETIME').str.strptime(pl.Datetime, format=date_fmt, strict=False).dt.year().alias('year')
            ).sort('ref_datetime')

            # Partition data by Month, Year and SYMBOL/ISIN
            month_year_partition_df = get_month_year_df.partition_by('year', 'month', instr_key_lowered)

            # Get all the end of month price data from master reference by Month, Year
            month_year_df = None
            for partition in month_year_partition_df:
                # Extending data frame one by one
                temp_df = pl.DataFrame(partition)
                eom_date = temp_df.select(pl.last("ref_datetime", "price", instr_key_lowered, "month", "year"))
                if month_year_df is None:
                    month_year_df = eom_date
                else:
                    month_year_df.extend(eom_date)
            # Price data from master reference
            filtered_month_year_df = month_year_df.select(pl.col(instr_key_lowered), pl.col('ref_datetime').sort(),
                                                          pl.col('price').alias('ref_price'), pl.col('month'),
                                                          pl.col('year'))
            return filtered_month_year_df
        except Exception as err:
            self.logger(msg=err)

    def get_view(self, query: str) -> pl.DataFrame:
        """
        Simple db read with polars to retrieve views
        """
        try:
            return pl.read_database(query, connection=conn, infer_schema_length=None)
        except Exception as err:
            self.logger(msg=err)

    def create_save_files(self, fund: str):
        """
        Creates save files for each fund within the external save folder
        """
        try:
            created, paths = [], []
            self.fund_report_path = os.path.join(self.price_reco_path, fund)
            self.equity_report_path = os.path.join(self.fund_report_path, 'Equities')
            self.bond_report_path = os.path.join(self.fund_report_path, 'Bonds')
            paths.extend((self.fund_report_path, self.equity_report_path, self.bond_report_path))

            for path in paths:
                os.mkdir(path)
                # created.append(os.path.exists(path))

            return True
        except Exception as err:
            self.logger(msg=err)

    def logger(self, msg):
        """
        Logging function for printing log
        """
        try:
            return print(f"{ str(datetime.now()) } - { msg }")
        except Exception as err:
            print(err)

    def get_instruments(self, positions: pl.dataframe, name: str):
        """
        Get all unique instrument symbol/isin from positions
        """
        try:
            return pl.Series(positions.filter(pl.col("financial_type") == name)["symbol"]).unique().sort().to_list()
        except Exception as err:
            self.logger(msg=err)


if __name__ == '__main__':
    reports = FundReportGenerator()
    asyncio.run(reports.main())
