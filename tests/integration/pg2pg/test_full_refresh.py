import time

import pytest
import sqlalchemy

import dbrep.engines.engine_sqlalchemy
import dbrep.utils
import dbrep.replication

def init_engine(user='postgres', password='postgres', db='postgres', host='127.0.0.1:5432'):
    conn_str = 'postgresql+psycopg2://{}:{}@{}/{}'.format(user, password, host, db)
    engine = sqlalchemy.create_engine(conn_str)
    try:
        conn = engine.connect()
        conn.close()
        return engine
    except:
        return None

def compare_outputs(out1, names1, out2, names2):
    if names1 != names2:
        raise Exception('Names should be the same, but got {} != {}!'.format(names1, names2))
    dbrep.utils.test_output_type(out1)
    dbrep.utils.test_output_type(out2)
    dbrep.utils.test_unique_key(out1)
    dbrep.utils.test_unique_key(out2)
    output = dbrep.utils.merge_outputs(out1, out2)
    tests = [
        dbrep.utils.test_elem_typing,
        dbrep.utils.test_elem_none,
        dbrep.utils.test_elem_type,
        dbrep.utils.test_elem_value,
    ]
    for test in tests:
        dbrep.utils.run_tests(output, test, report_cols=True)

def compare_results(cursor1, cursor2):
    def get_output_(cur):
        names = list(cur.keys())
        out = [list(x) for x in cur.fetchall()]
        return names, out
    names1, out1 = get_output_(cursor1)
    names2, out2 = get_output_(cursor2)
    return compare_outputs(out1, names1, out2, names2)

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
    
    src_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': 'postgresql+psycopg2://postgres:postgres@test-postgres:5432/postgres'})
    dst_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': 'postgresql+psycopg2://postgres:postgres@test-postgres:5432/postgres'})
    dbrep.replication.full_refresh(src_engine, dst_engine, {
        'src': {'table': 'test_src'},
        'dst': {'table': 'test_dst'}
    })

    compare_results(engine.execute('select * from test_src'), engine.execute('select * from test_dst'))