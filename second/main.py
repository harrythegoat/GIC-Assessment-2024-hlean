from sqlalchemy import create_engine
import pandas as pd
import xlwings as xw
import matplotlib.pyplot as plt

pd.set_option('display.width', 700)
pd.set_option('display.max_columns', 15)
pd.set_option('display.precision', 2)

db_uri = r'postgresql://postgres:admin123@localhost:5432/gic_funds'
db = create_engine(db_uri)

query = "SELECT applebead_price_diff('TJX')"
table = pd.read_sql(query, db).to_dict('list')

df = pd.DataFrame(columns=["SYMBOL", "REF DATE", "REF PRICE", "DATE", "FUND PRICE", "REALISED P/L", "MARKET VALUE",
                           "INITIAL MARKET VALUE", "ENTRY PRICE", "QUANTITY", "PRICE DIFF", "BREAK PERCENTILE"])
for k, v in table.items():
    for i in range(0, len(v)):
        row = v[i][1:-1].split(",")
        df.loc[i] = row

# df['REALISED P/L'] = df['REALISED P/L'].apply(lambda x: float("{:.2f}".format(x)))
# df['MARKET VALUE'] = df['MARKET VALUE'].apply(lambda x: float("{:.2f}".format(x)))
# df['INITIAL MARKET VALUE'] = df['INITIAL MARKET VALUE'].apply(lambda x: float("{:.2f}".format(x)))
# df['ENTRY PRICE'] = df['ENTRY PRICE'].apply(lambda x: float("{:.2f}".format(x)))
# df['QUANTITY'] = df['QUANTITY'].apply(lambda x: float("{:.2f}".format(x)))
# df['PRICE DIFF'] = df['PRICE DIFF'].apply(lambda x: float("{:.2f}".format(x)))
# df['BREAK PERCENTILE'] = df['BREAK PERCENTILE'].apply(lambda x: float("{:.2f}".format(x)))

report_path = r"C:\Users\PC\Desktop\Git\gic_assessment\second\funds_csv\applebead\equities\tjx.xlsx"
sheet_name = "TJX"
df_mapping = {"A1": df}

with xw.App(visible=False) as app:
    wb = app.books.open(report_path)
    current_sheets = [sheet.name for sheet in wb.sheets]
    if sheet_name not in current_sheets:
        wb.sheets.add(sheet_name)

    for cell_target, df in df_mapping.items():
        wb.sheets(sheet_name).range("A1:L1").column_width = 20
        wb.sheets(sheet_name).range('A1:L14').api.Borders.Weight = 1
        wb.sheets(sheet_name).range(cell_target).options(pd.DataFrame, index=False).value = df

        fig = plt.figure(figsize=(16, 5))
        df.set_index("DATE")
        plt.xlabel("Date")
        plt.ylabel("Price Difference")
        plt.plot(df["DATE"], df["PRICE DIFF"])
        wb.sheets(sheet_name).pictures.add(fig, name="TJX Price Difference", update=True, left=wb.sheets(sheet_name).range('B17').left, top=wb.sheets(sheet_name).range('B17').top)

    wb.save()

# df.set_index("DATE")
# plt.plot(df["DATE"], df["PRICE DIFF"])
# plt.show()