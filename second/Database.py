import os, re
import pandas as pd
import polars as pl
import dateutil.parser
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine, Column, Integer, String, DOUBLE_PRECISION, Date, Float, insert, delete, update

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class FundsReport(Base):
    __tablename__ = "funds_report"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    fund = Column(String)
    financial_type = Column(String)
    symbol = Column(String)
    security_name = Column(String)
    sedol = Column(String)
    isin = Column(String)
    price = Column(DOUBLE_PRECISION)
    quantity = Column(DOUBLE_PRECISION)
    realised_pl = Column(DOUBLE_PRECISION)
    market_value = Column(DOUBLE_PRECISION)
    datetime = Column(Date)

class PriceDifference(Base):
    __tablename__ = "price_difference"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    fund = Column(String)
    security_type = Column(String)
    security_name = Column(String)
    instrument_id = Column(String)
    ref_price = Column(DOUBLE_PRECISION)
    ref_datetime = Column(String)
    price = Column(DOUBLE_PRECISION)
    datetime = Column(String)
    price_diff = Column(DOUBLE_PRECISION)
    last_updated = Column(String)


# Create the table in the database
Base.metadata.create_all(engine, checkfirst=True)


def load_external_funds():
    folder_path = r"C:\Users\PC\Desktop\Git\gic_assessment\external-funds"
    reports = sorted(os.path.join(folder_path, x) for x in os.listdir(folder_path))

    funds = ['Whitestone', 'Wallington', 'Catalysm', 'Belaware',
             'Gohen', 'Applebead', 'Magnum', 'Trustmind', 'Leeder',
             'Virtous']

    funds_reports = {}
    for fund in funds:
        name = fund.upper()
        temp = [x for x in reports if name in x.upper()]
        funds_reports[name] = temp

    for fund_name, reports in funds_reports.items():
        for report in reports:
            temp_date = re.findall(r'\d+', report)
            if len(temp_date) == 1:
                temp = str(dateutil.parser.parse(temp_date[0])).split()[0]
                formatted = datetime.strptime(temp, '%Y-%m-%d').date()
                print("formatted", formatted)
                final_date = formatted
            else:
                convert = [int(x) for x in temp_date]
                final_date = '/'.join([str(x) for x in sorted(convert)])
                final_date = datetime.strptime(final_date, '%m/%d/%Y').date()
            report_csv = pd.read_csv(report, encoding='utf-8')
            report_csv.insert(0, "fund", "")
            report_csv["fund"] = fund_name
            report_csv["datetime"] = final_date
            report_csv.rename(columns={
                "FINANCIAL TYPE": "financial_type",
                "SYMBOL": "symbol",
                "SECURITY NAME": "security_name",
                "SEDOL": "sedol",
                "ISIN": "isin",
                "PRICE": "price",
                "QUANTITY": "quantity",
                "REALISED P/L": "realised_pl",
                "MARKET VALUE": "market_value"
            }, inplace=True)
            print(report_csv)
            report_csv.to_sql("funds_report", con=engine, if_exists="append", index=False)
    session.commit()
    session.close()


def insert_row(table: declarative_base, values: dict):
    stmt = (
        insert(table).values(values)
    )
    print(f"Executing {stmt}")
    session.execute(stmt)
    session.commit()
    session.close()


def update_row(table: declarative_base, columns: tuple, values: dict):
    stmt = (
        update(table).where(*columns).values(values)
    )
    print(f"Executing {stmt}")
    session.execute(stmt)
    session.commit()
    session.close()


def delete_row(table: declarative_base, conditions: tuple):
    stmt = (
        delete(table).where(*conditions)
    )
    print(f"Executing {stmt}")
    session.execute(stmt)
    session.commit()
    session.close()


if __name__ == '__main__':
    # uncomment to load external_funds data
    # load_external_funds()
    read_external_funds = pl.read_database(query=os.getenv("EXTERNAL_FUNDS"), connection=session, infer_schema_length=None)
    session.close()
    print(read_external_funds)
