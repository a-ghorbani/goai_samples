
import jaydebeapi
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TJSONProtocol
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.transport import TTransport
from mapd import MapD

def connect_jdbc(dbname, user, host, password, jar='./mapdjdbc-1.0-SNAPSHOT-jar-with-dependencies.jar' ):
    try:
        return jaydebeapi.connect('com.mapd.jdbc.MapDDriver',
                                  'jdbc:mapd:{}:{}:'.format(host, dbname),
                                  {'user': user, 'password': password},
                                  jar)
    except Exception as e:
        print('Error: {}'.format(str(e)))
        raise e

def get_client(host_or_uri, port, http):
    if http:
        transport = THttpClient.THttpClient(host_or_uri)
        protocol = TJSONProtocol.TJSONProtocol(transport)
    else:
        socket = TSocket.TSocket(host_or_uri, port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = MapD.Client(protocol)
    transport.open()
    return client

def get_create_table_query_fmt(df, table_name):
    cols_fmt = ""
    for col in df.columns:
        t = 'TEXT'
        if df[col].dtype.kind == 'i':
            t = "INT"
        elif df[col].dtype.kind == 'f':
            t = "FLOAT"
        cols_fmt += ("%s %s, " % (col, t))
    return "create table " + table_name + " ( " + cols_fmt.strip(', ') + " ); "

def get_insert_query_fmt(df, table_name):
    cols_fmt = ""
    for col in df.columns:
        it = "'{}'"
        if df[col].dtype.kind in 'iuf':
            it = "{}"
        cols_fmt += it + ", "
    iq = "insert into " + table_name + " values( " + cols_fmt.strip(', ') + " ); "
    return iq

def create_table_for(session, client, df, table_name):
    create_q = get_create_table_query_fmt(df,table_name)
    print("Create query: ", create_q)
    return client.sql_execute(session, create_q, True, None, -1)

def insert_df_T(session, client, df, table_name):
    input_rows = [MapD.TStringRow(cols=[MapD.TStringValue(str(r)) for r in row]) 
                  for index, row in df.iterrows()]
    client.load_table(session, table_name, input_rows)

def insert_df(session, client, insert_fmt, df):
    import re
    for index, row in df.iterrows():
        insert_q = insert_fmt.format(*row.values)
        insert_q = re.sub("None", " NULL ", insert_q)
        client.sql_execute(session, insert_q, True, None, -1)
