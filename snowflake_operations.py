import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
import pandas as pd

def snowflake_connection(connection_path:str):
    """
    Establishes and returns a connection to a Snowflake instance.

    Parameters:
    - connection_path (str): Path to a JSON file containing connection details 
                             like User, Password, Account, Warehouse, Database, and Schema.

    Returns:
    - snowflake.connector.connection: A Snowflake connection object.

    Notes:
    - The JSON file should have the following keys: User, Password, Account, Warehouse, Database, and Schema.
    """
    with open(connection_path,'+r') as con_file:
        data = json.load(con_file)
    conn = snowflake.connector.connect(
    user=data['User'],
    password=data['Password'],
    account=data['Account'],
    warehouse=data['Warehouse'],
    database=data['Database'],
    schema=data['Schema']
    )
    return conn


def write_to_db(snowflake_conn:snowflake.connector.connection,table:str,df:pd.DataFrame,if_exists='replace'):
    """
    Writes a pandas DataFrame to a specified Snowflake table.

    The function creates the Snowflake table (or replaces it if it already exists) 
    and inserts the records from the DataFrame.

    Parameters:
    - snowflake_conn (snowflake.connector.connection): A valid Snowflake connection object.
    - table (str): Name of the Snowflake table to write data to.
    - df (pd.DataFrame): The DataFrame containing data to be written.
    - if_exists (str, optional): What to do if the table already exists. Defaults to 'replace'.
                                 Possible values: 'replace' or any other string value for append operation.

    Returns:
    - None: The function prints "Write Completed" upon successful execution, else prints the error message.

    Notes:
    - This function has built-in datatype mapping from pandas to Snowflake data types.
    - If `if_exists` is set to 'replace', the function will drop the table if it exists, then create a new one.
    - Any lists in the DataFrame will be converted to strings separated by dashes.
    """
    new_df = list(zip(df.columns,df.dtypes))
    new_df = [(i[0],'string') if i[1] =='object' else (i[0],i[1]) for i in new_df]
    new_df = [(i[0],'int') if 'int' in str(i[1]) else (i[0],i[1]) for i in new_df]
    new_df = [(i[0],'float') if 'float' in str(i[1]) else (i[0],i[1]) for i in new_df]
    columns = [str(i[0])+' '+str(i[1])+'\n' for i in new_df]
    script = f"""
    CREATE TABLE {table}
    (
    {','.join(columns)}
    );
    """
    cursor = snowflake_conn.cursor()
    if if_exists == 'replace':
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(script)
    else:
        for i in df.columns:
            if df[i].apply(lambda x: isinstance(x, list)).any():
                df[i] = df[i].apply(lambda x: '-'.join(x))
                
        for _, row in df.iterrows():
            query = f"INSERT INTO {table} ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
            values = tuple(row[col] for col in df.columns)
            cursor.execute(query,values)
            run = True
    try:
        if run:
            pass
        else:
            write_pandas(snowflake_conn,df,table_name=table,quote_identifiers=False)
            run = True
    except Exception as e:
            if run:
                pass
            else:
                write_pandas(snowflake_conn,df,table_name=table,quote_identifiers=True)
                run = True 
    finally:
        if run:
            print("Write Completed")
        else:
            print(str(e)+"Check inputs and code")

