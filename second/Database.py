import os, re
import pandas as pd
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

class ProgName(Base):
    __tablename__ = "prog_name"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    unit_nbr = Column(Integer)
    step_seq_id = Column(Integer)
    step_prog_name = Column(String)

class DependencyRule(Base):
    __tablename__ = "dependency_rule"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    unit_nbr = Column(Integer)
    rule_id = Column(Integer)
    step_seq_id = Column(Integer)
    step_dep_id = Column(Integer)

# Create the table in the database
Base.metadata.create_all(engine, checkfirst=True)


def load_external_funds():
    folder_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'external-funds'))
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

def load_prog_name():
    prog_names = [
        {"unit_nbr": 1, "step_seq_id": 1, "step_prog_name": "PKGIDS_CMMN_UTILITY.PROCIDS_JOB_START"},
        {"unit_nbr": 1, "step_seq_id": 2, "step_prog_name": "pkgids_ptf_hrchy_processing.Procids_delete_job_set_nbr"},
        {"unit_nbr": 1, "step_seq_id": 3, "step_prog_name": "PKGIDS_PTF_EXTR.ext_static_ptf_table"},
        {"unit_nbr": 1, "step_seq_id": 4, "step_prog_name": "PKGIDS_PTF_EXTR.ext_eff_ptf_table"},
        {"unit_nbr": 1, "step_seq_id": 5, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_tree_a"},
        {"unit_nbr": 1, "step_seq_id": 6, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_tree_b"},
        {"unit_nbr": 1, "step_seq_id": 7, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_tree_c"},
        {"unit_nbr": 1, "step_seq_id": 8, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_tree_d"},
        {"unit_nbr": 1, "step_seq_id": 9, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_tree_e"},
        {"unit_nbr": 1, "step_seq_id": 10, "step_prog_name": "pkgids_ptf_hrchy_processing.procids_get_active_portf"},
        {"unit_nbr": 1, "step_seq_id": 11, "step_prog_name": "pkgids_ptf_lineage.procids_process_ptf_lineage"},
        {"unit_nbr": 1, "step_seq_id": 12, "step_prog_name": "pkgids_ptf_lineage.procids_summary_to_bookable_rs"},
        {"unit_nbr": 1, "step_seq_id": 13, "step_prog_name": "PKGIDS_CMMN_UTILITY.PROCIDS_JOB_END"}
    ]
    for name in prog_names:
        insert_row(ProgName, name)

def load_dependency_rule():
    dependency_rules = [
        {"unit_nbr": 1, "rule_id": 1, "step_seq_id": 1, "step_dep_id": 0},
        {"unit_nbr": 1, "rule_id": 2, "step_seq_id": 2, "step_dep_id": 1},
        {"unit_nbr": 1, "rule_id": 3, "step_seq_id": 3, "step_dep_id": 2},
        {"unit_nbr": 1, "rule_id": 4, "step_seq_id": 4, "step_dep_id": 2},
        {"unit_nbr": 1, "rule_id": 5, "step_seq_id": 5, "step_dep_id": 3},
        {"unit_nbr": 1, "rule_id": 6, "step_seq_id": 5, "step_dep_id": 4},
        {"unit_nbr": 1, "rule_id": 7, "step_seq_id": 6, "step_dep_id": 3},
        {"unit_nbr": 1, "rule_id": 8, "step_seq_id": 6, "step_dep_id": 4},
        {"unit_nbr": 1, "rule_id": 9, "step_seq_id": 7, "step_dep_id": 3},
        {"unit_nbr": 1, "rule_id": 10, "step_seq_id": 7, "step_dep_id": 4},
        {"unit_nbr": 1, "rule_id": 11, "step_seq_id": 8, "step_dep_id": 3},
        {"unit_nbr": 1, "rule_id": 12, "step_seq_id": 9, "step_dep_id": 3},
        {"unit_nbr": 1, "rule_id": 13, "step_seq_id": 8, "step_dep_id": 4},
        {"unit_nbr": 1, "rule_id": 14, "step_seq_id": 9, "step_dep_id": 4},
        {"unit_nbr": 1, "rule_id": 15, "step_seq_id": 10, "step_dep_id": 5},
        {"unit_nbr": 1, "rule_id": 16, "step_seq_id": 10, "step_dep_id": 6},
        {"unit_nbr": 1, "rule_id": 17, "step_seq_id": 10, "step_dep_id": 7},
        {"unit_nbr": 1, "rule_id": 18, "step_seq_id": 10, "step_dep_id": 8},
        {"unit_nbr": 1, "rule_id": 19, "step_seq_id": 10, "step_dep_id": 9},
        {"unit_nbr": 1, "rule_id": 20, "step_seq_id": 11, "step_dep_id": 10},
        {"unit_nbr": 1, "rule_id": 21, "step_seq_id": 12, "step_dep_id": 11},
        {"unit_nbr": 1, "rule_id": 22, "step_seq_id": 13, "step_dep_id": 12},
    ]
    for name in dependency_rules:
        insert_row(DependencyRule, name)



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
    # Run (ONLY) once for both the assessment
    load_external_funds()
    load_prog_name()
    load_dependency_rule()
