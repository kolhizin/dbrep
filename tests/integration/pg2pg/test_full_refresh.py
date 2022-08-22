import time

import pytest
import sqlalchemy

import dbrep.engines.engine_sqlalchemy

def init_engine(user='postgres', password='postgres', db='postgres', host='127.0.0.1:5432'):
    conn_str = 'postgresql+psycopg2://{}:{}@{}/{}'.format(user, password, host, db)
    engine = sqlalchemy.create_engine(conn_str)
    try:
        conn = engine.connect()
        conn.close()
        return engine
    except:
        return None

def test_simple():
    engine = init_engine(host='test-postgres:5432')
    if not engine:
        raise ConnectionError('DB is not available!')
        
    engine.execute("drop table if exists test_src")
    engine.execute("drop table if exists test_dst")
    engine.execute("""
    create table test_src (
        rid serial,
        col_int integer,
        col_str varchar(255)
    )
    """)
    engine.execute("""
    create table test_dst (
        rid integer,
        col_int integer,
        col_str varchar(255)
    )
    """)
    engine.execute("insert into test_src (col_int) values (1), (2), (3)")
    engine.execute("insert into test_src (col_str) values ('a'), ('b'), ('c')")

    assert len(engine.execute("select * from test_src").fetchall()) == 6