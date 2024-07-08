# - Whitestone
# - Wallington
# - Catalysm
# - Belaware
# - Gohen
# - Applebead
# - Magnum
# - Trustmind
# - Leeder
# - Virtous
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
import os, re
from datetime import datetime, timedelta
import dateutil.parser

def infer_sqlalchemy_type(dtype):
    """ Map pandas dtype to SQLAlchemy's types """
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(255)
    else:
        return String(255)

funds = ['Whitestone', 'Wallington', 'Catalysm', 'Belaware',
         'Gohen', 'Applebead', 'Magnum', 'Trustmind', 'Leeder',
         'Virtous']

dir_path = r"C:\Users\PC\Downloads\data-engineering\data-engineering\external-funds"
reports = sorted(os.path.join(dir_path, x) for x in os.listdir(dir_path))
funds_reports = {}

print(reports)

for fund in funds:
    name = fund.lower()
    temp = [x for x in reports if name in x.lower()]
    funds_reports[name] = temp


db_uri = r'postgresql://postgres:admin123@localhost:5432/gic_funds'
engine = create_engine(db_uri)

for fund_name, reports in funds_reports.items():
    for report in reports:
        temp_date = re.findall(r'\d+', report)
        if len(temp_date) == 1:
            temp = str(dateutil.parser.parse(temp_date[0])).split()[0]
            formatted = datetime.strptime(temp, '%Y-%m-%d').date()
            final_date = formatted.strftime('%m/%d/%Y')
        else:
            convert = [int(x) for x in temp_date]
            final_date = '/'.join([str(x) for x in sorted(convert)])
        final_date = datetime.strptime(final_date, '%m/%d/%Y').date()
        print(final_date)
        report_csv = pd.read_csv(report, encoding='utf-8')
        report_csv['DATE'] = final_date
        metadata = MetaData()
        columns = [Column(str(name), infer_sqlalchemy_type(dtype)) for name, dtype in report_csv.dtypes.items()]
        table = Table(fund_name.lower(), metadata, *columns)
        table.create(engine, True)
        report_csv.to_sql(fund_name.lower(), con=engine, if_exists='append', index=False)

# for fund in funds:
#     print("FUND NAME -> {}".format(fund))
#     fund_name = fund.lower()
#     sql_query = 'SELECT * FROM {}'.format(fund_name)
#     table_data = pd.read_sql(sql_query, engine)
#     print(len(table_data))
#     print(table_data)
#     print(table_data.to_dict('list'))

# sql_query = 'SELECT * FROM applebead ORDER BY "DATE"'
# table_data = pd.read_sql(sql_query, engine)
# data = table_data.to_dict('list')
# equities = {}
# bonds = {}
# for i in range(0, len(data['SYMBOL'])):
#     # equity financial type (mm/dd/yyyy)
#     # bond financial type (yyyy-mm-dd)
#     symbol = data['SYMBOL'][i]
#     date = data['DATE'][i]
#     f_type = data['FINANCIAL TYPE'][i]
#     price = data['PRICE'][i]
#     realised_pl = float(data['REALISED P/L'][i])
#     market_value = float(data['MARKET VALUE'][i])
#     quantity = float(data['QUANTITY'][i])
#     # print(symbol, date, f_type)
#     if 'Equities' in f_type:
#         date_formatted = datetime.strptime(date, '%Y-%m-%d').date()
#         equity_date = date_formatted.strftime('%m/%d/%Y')
#         if symbol not in equities:
#             equities[symbol] = []
#         # if postive  realised p / l (market value - (+realised p / l)) / quantity
#         # if negative realised p / l (market value + (-realised p / l) ) / quantity
#         if realised_pl > 0:
#             entry_price = (market_value - realised_pl) / quantity
#         else:
#             entry_price = (market_value + abs(realised_pl)) / quantity
#         equities[symbol].append({
#             'date': equity_date.lstrip('0'),
#             'price': price,
#             'instrument': f_type,
#             'realised_pl': realised_pl,
#             'market_value': market_value,
#             'quantity': quantity,
#             'entry': entry_price,
#             'break': abs(float(entry_price - price)) if realised_pl > 0 else -float(entry_price - price),
#             'total_position': entry_price * quantity
#         })
#     elif 'Government Bond' in f_type:
#         if symbol not in bonds:
#             bonds[symbol] = []
#         bonds[symbol].append({
#             'date': date,
#             'price': price,
#             'instrument': f_type
#         })
#
# print('Equities -> {}'.format(equities))
# print('Bonds -> {}'.format(bonds))
#
# prev_equity_price = 0
# prev_equity_date = None
# total_pl = 0
# for dt in equities['TJX']:
#     # Handle if price data not available
#     temp = dt['date']
#     while True:
#         # print('TJX Runs -> {}'.format(temp))
#         price_query = """SELECT * FROM equity_prices WHERE "SYMBOL"='{}' AND "DATETIME"='{}';""".format('TJX', temp)
#         table_data = pd.read_sql(price_query, engine)
#         res = table_data.to_dict('list')
#         if len(res['PRICE']) > 0:
#             # print("FUND REPORT {}: {} | REF TABLE {}: {}".format(temp, dt['price'], res['DATETIME'][0], res['PRICE'][0]))
#             # TICKER, BREAK, ENTRY, EXIT, CLOSING (MARKET), REALISED PL, ENTRY MARKETVAL, EXIT MARKETVAL, QUANTIY, DATE
#             print("(At.M.V: {} / Post.M.V: {} / Realised.PL: {} | BREAK: {} | ENTRY: {} | EXIT: {} | REALISED P/L: {} | QUANTITY: {} | MARKET CLOSING PRICE: {}".
#                   format(dt['total_position'], dt['market_value'], dt['market_value']-dt['total_position'], dt['break'], dt['entry'], dt['price'], dt['realised_pl'], dt['quantity'], res['PRICE'][0]))
#             total_pl += dt['realised_pl']
#             break
#         sub_one = datetime.strptime(temp, '%m/%d/%Y').date() - timedelta(days=1)
#         temp = sub_one.strftime('%m/%d/%Y').lstrip('0')
#     prev_equity_price = dt['price']
#     prev_equity_date = temp
#     total_pl += dt['realised_pl']
# print(total_pl)
#
# print('\n')
# prev_bond_price = 0
# prev_bond_date = None
# for dt in bonds['NL0011819040']:
#     temp = dt['date']
#     while True:
#         price_query = """SELECT * FROM bond_prices WHERE "ISIN"='{}' AND "DATETIME"='{}';""".format('NL0011819040', temp)
#         table_data = pd.read_sql(price_query, engine)
#         res = table_data.to_dict('list')
#         if len(res['PRICE']) > 0:
#             # print("{}: {} - {}: {} | Break -> {}".format(temp, res['PRICE'][0], prev_bond_date, prev_bond_price, float(res['PRICE'][0]-prev_bond_price)))
#             break
#         subtract_dt = datetime.strptime(temp, '%Y-%m-%d').date() - timedelta(days=1)
#         temp = subtract_dt.strftime('%Y-%m-%d')
#     prev_bond_price = dt['price']
#     prev_bond_date = temp



#     symbol = data['SYMBOL'][i]
#     financial_type = data['FINANCIAL TYPE'][i]
#     date = data['DATE'][i]
#     if symbol in temp:
#         pass
#         # temp[symbol][date] = dict()
#         # temp[symbol][date]['break'] = 0
#         # temp[symbol][date]['financial_type'] = financial_type
#     else:
#         temp[symbol] = {}
#         if 'Equities' in financial_type:
#             date_formatted = datetime.strptime(date, '%Y-%m-%d').date()
#             equity_date = date_formatted.strftime('%m/%d/%Y')
#             # print(equity_date)
#             price_query = """SELECT * FROM equity_prices WHERE "SYMBOL"='{}' AND "DATETIME"='{}';""".format(symbol, equity_date)
#             print(price_query)
#             # table_data = pd.read_sql(price_query, engine)
#             # res = table_data.to_dict('list')
#             # print(res['PRICE'][0])
#         elif 'Government Bond' in financial_type:
#             pass

# price_query = """SELECT * FROM equity_prices WHERE "SYMBOL"='{}' AND "DATETIME"='{}';""".format('TJX', '2/28/2023')
# table_data = pd.read_sql(price_query, engine)
# res = table_data.to_dict('list')
# print(type(res['PRICE'][0]))
