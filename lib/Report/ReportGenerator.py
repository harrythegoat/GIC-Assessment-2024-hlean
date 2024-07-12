import asyncio, os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
from datetime import datetime
from tabulate import tabulate

class ReportGenerator:
    def __init__(self):
        load_dotenv()
        self.tasks = list()
        self.funds = list()
        self.db_name = os.getenv('DB_NAME')
        self.db_username = os.getenv("DB_USERNAME")
        self.db_pwd = os.getenv("DB_PASSWORD")
        self.db_uri = "postgresql://{}:{}@localhost:5432/{}".format(self.db_username, self.db_pwd, self.db_name)
        self.db = create_engine(self.db_uri)

    def run(self):
        # Get Funds
        self.funds = self.get_funds()
        print(self.funds)
        asyncio.run(self.main())

    async def main(self):
        for fund in self.funds:
            temp = asyncio.create_task(self.generate_report(fund=fund))
            self.tasks.append(temp)
        check_tasks = [await task for task in self.tasks]
        print('All Completed' if all(check_tasks) else 'Failed')

    async def generate_report(self, fund=""):
        print("GENERATING {} FUND REPORT".format(fund))
        # Get Equities, Bonds and Cash
        general_queries = {
            'equities': "SELECT DISTINCT \"SYMBOL\" FROM {} WHERE \"FINANCIAL TYPE\"='Equities' ORDER BY \"SYMBOL\"".format(fund),
            'g_bonds': "SELECT DISTINCT \"SYMBOL\" FROM {} WHERE \"FINANCIAL TYPE\"='Government Bond' ORDER BY \"SYMBOL\"".format(fund),
            'cash': "SELECT \"DATE\", \"MARKET VALUE\" FROM {} WHERE \"FINANCIAL TYPE\"='CASH' ORDER BY \"DATE\"".format(fund)
        }
        report_data = dict()
        for key, query in general_queries.items():
            report_data[key] = await self.read_sql(query=query)

        equity_symbols = report_data['equities']['SYMBOL']
        # g_bond_symbols = report_data['g_bonds']['SYMBOL']

        # Get SYMBOL Data for Equities
        query_columns = ['"SYMBOL"', '"PRICE"', '"QUANTITY"', '"REALISED P/L"', '"MARKET VALUE"', '"DATE"']
        equity_data = dict()
        for equity in report_data['equities']['SYMBOL']:
            query = "SELECT {} FROM {} WHERE \"{}\"='{}' ORDER BY \"{}\"".format(",".join(query_columns), fund,
                                                                                 "SYMBOL", equity, "DATE")
            result = await self.read_sql(query=query)
            equity_data[equity] = result

        # Print every symbol fund's data
        columns = ["FUND", "SYMBOL", "DATE", "ENTRY", "PRICE", "BREAK", "REALISED P/L", "MARKET VALUE", "ACCU P/L"]
        for equity in equity_symbols:
            df = pd.DataFrame()
            title = "{} Fund Report for {}".format(fund, equity)
            for key, value in equity_data[equity].items():
                se_list = pd.Series(value)
                df[key] = se_list.values
            df["ENTRY"] = round(df["MARKET VALUE"].add(-df["REALISED P/L"]).divide(df["QUANTITY"]), 2)
            df["BREAK"] = df["PRICE"].sub(df["ENTRY"])
            df["FUND"] = fund.capitalize()
            df["MARKET VALUE"].apply(lambda x: '%.20f' % x).values.tolist()
            print(df["MARKET VALUE"])
            acc_pl = pd.Series(df["REALISED P/L"])
            df["ACCU P/L"] = acc_pl.cumsum()
            # df.rename(columns={"PRICE": "CLOSE"}, inplace=True)
            df = df.reindex(columns=columns)
            # df.set_index("BREAK")
            # now = datetime.now()
            # df.to_csv(r"C:\Users\PC\Desktop\Git\gic_assessment\second\funds_csv\{}\equities\{}_{}_{}.csv".format(fund.lower(), fund.upper(), equity, now.strftime("%d_%m_%Y_%H_%M")), index=False)
            # print(title)
            print(tabulate(df, headers=columns, showindex=False, tablefmt="fancy_outline", colalign=("center",), maxcolwidths=[None, 25]))
            # await self.write_sql(data=df, table="Reports")
        # Get SYMBOL Data for Equities
        # bond_data = dict()
        # for bond in report_data['g_bonds']['SYMBOL']:
        #     print(bond)
        #     query = "SELECT {} FROM {} WHERE \"{}\"='{}' ORDER BY \"{}\"".format(",".join(query_columns), fund,
        #                                                                          "SYMBOL", bond, "DATE")
        #     result = await self.read_sql(query=query)
        #     bond_data[bond] = result
        #     print(result)


        return True

    def get_funds(self):
        query = 'SELECT * FROM Funds ORDER BY NAME'
        table = pd.read_sql(query, self.db).to_dict('list')
        funds = table['name']
        return funds

    async def read_sql(self, query=""):
        table = pd.read_sql(query, self.db).to_dict('list')
        return table

    async def write_sql(self, data, table):
        data.to_sql(table, con=self.db, if_exists='append', index=False)
        return True

    def infer_sqlalchemy_type(self, dtype):
        """ Map pandas dtype to SQLAlchemy's types """
        if "int" in dtype.name:
            return Integer
        elif "float" in dtype.name:
            return Float
        elif "object" in dtype.name:
            return String(255)
        else:
            return String(255)

if __name__ == "__main__":
    report = ReportGenerator()
    report.run()

