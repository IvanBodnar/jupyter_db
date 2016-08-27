import psycopg2
from psycopg2 import extras
import pandas as pd

conn_dict = {
    'host': 'localhost',
    'dbname': 'pfa_dgc_nueva',
    'user': 'ivan',
    'password': 'ivan'
}


class Records:
    """
    Conecta con la base de datos y realiza consultas

    :param conn_dict: Diccionario con los parametros de conexión
                      a la base de datos:
                      > host
                      > dbname
                      > user
                      > password
    :param: table: string con el nombre de la tabla
    :param: query: string con la consulta a realizar
    :param: schema='public': string con el nombre del schema
    """
    def __init__(self, conn_dict, table, query, schema='public'):
        self.schema = schema
        self.table = table
        self.query = query
        self.conn = psycopg2.connect(**conn_dict)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.cur.execute(self.query)
        self.executed = self.cur.fetchall()

    def records(self):
        """
        Devuelve una lista de objetos 'record' con los datos
        de la consulta

        :param query: string con la consulta a ejecutar
        :return: list de namedtuples
        """
        return [row for row in self.executed]

    def dicts(self):
        """
        Devuelve una lista de ordereddicts
        :return: list de ordereddicts
        """
        return [row._asdict() for row in self.executed]

    def pds_df(self):
        """
        Devuelve un DataFrame de pandas
        :return: pandas DataFrame
        """
        return pd.DataFrame.from_dict([row._asdict() for row in self.executed])


# q = '''
#     select anio, count(*)
#     from hechos
#     group by anio
#     order by anio;
# '''
#
# q1 = 'select lugar_hecho from hechos limit 10'
#
# rec = Records(conn_dict, 'hechos', q)
#
# print(rec.records())

# conn = psycopg2.connect(**conn_dict)
# cur = conn.cursor()
# cur.execute(q)
#
# print([x[0] for x in cur.fetchall()])

