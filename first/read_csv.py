from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
import pandas as pd
dependency_rule_csv_path = r"C:\Users\PC\Downloads\dependency_rule.csv"
prog_name_csv_path = r"C:\Users\PC\Downloads\prog_name.csv"

dependency_rule_df = pd.read_csv(dependency_rule_csv_path, encoding='utf-8')
prog_name_df = pd.read_csv(prog_name_csv_path, encoding='utf-8')

db_uri = r'postgresql://postgres:admin123@localhost:5432/gic_funds'
engine = create_engine(db_uri)

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

metadata = MetaData()
columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in prog_name_df.dtypes.items()]
table = Table('prog_name', metadata, *columns)
table.create(engine, True)
prog_name_df.to_sql('prog_name', con=engine, if_exists='append', index=False)

sql_query = 'SELECT * FROM prog_name'
table_data = pd.read_sql(sql_query, engine)
print(len(table_data))
print(table_data)
print(table_data.to_dict('list'))

metadata = MetaData()
columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in dependency_rule_df.dtypes.items()]
table = Table('dependency_rule', metadata, *columns)
table.create(engine, True)
dependency_rule_df.to_sql('dependency_rule', con=engine, if_exists='append', index=False)

sql_query = 'SELECT * FROM dependency_rule'
table_data = pd.read_sql(sql_query, engine)
print(len(table_data))
print(table_data)
print(table_data.to_dict('list'))

engine.dispose()