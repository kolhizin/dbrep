from distutils.command import check
import functools
import time

import pytest
import sqlalchemy

import dbrep.engines.engine_sqlalchemy
import dbrep.utils
import dbrep.replication
import utils

config = utils.read_config()

src_config = config['connections']['postgres']
dst_config = config['connections']['postgres']
tests = {x['name']:x for x in config['tests']
            if x['src']['config']['conn'] == 'postgres' and x['dst']['config']['conn'] == 'postgres'}

src_objects = ['test_src']
dst_objects = ['test_dst']

"""
We have several layers of abstraction:

1. Test driver (src & dst) - it should not be visible in pytest report at all
2. Engine (src & dst) - it should be visible in pytest report
3. Different DBs (src & dst) - it may or may not be visible in pytest report
4. Different configs - it may not be visible in pytest report
5. Different use cases - it may not be visible in pytest report

Hence:
1. Test driver should be determined based on test definition (basically two of them: SQLA and Kafka).
    - Must be hidden
    - Must handle clean-up
2. Engine - make test_engine2engine.py file
3. Different DBs - may be parameters to actual docker-compose.yaml file 

Must decouple DB state from Engine.
SRC/DST -> only one Test Driver -> many Engines

PGTemplate:
    driver: postgres
    setup:
        - create table ()
    dst:
        - driver: ch
          setup: []
          params: []

templates: [PGTemplate, CHTemplate, etc.]
steps:
 - insert into values

template: PGTemplate
steps:
 - insert into values

Test-case:
    driver: actual-DB (e.g. ClickHouse, Postgres or Kafka => driver)
    steps: [a, b, c, d]
    configs: #possible Engines
        - conn: a
          params: ...
    dst:
        driver: actual-DB
        steps: [a, b, c, d]
        configs: #possible Engines
            - conn: a
              params: ...

Test:
    src-driver: SQLAlchemy/Kafka + actual connection string
    dst-driver: SQLAlchemy/Kafka + actual connection string
    src-conn: A
    dst-conn: B
    #test_A_to_B.py -> single fixtures
    src-config: ...
    dst-config: ...

test_incremental
test_full_refresh
test_infer_incremental
test_infer_full_refresh

1 step: create explicit tests
2 step: create src -> dst structure
3 step: create templates
"""


@pytest.fixture(scope='function', params=list(tests.keys()))
def connection(request):
    test_name = request.param
    test_config = tests[test_name]
    src_driver = 
    driver = sqlalchemy.create_engine(src_config['conn-str'])
    conn = driver.connect()

    yield conn
    utils.sqla_cleanup_tables(conn, src_objects)
    conn.close()
    driver.dispose()

"""
@pytest.fixture(scope='function', params=list(tests.keys()))
def src_driver(request):
    driver = sqlalchemy.create_engine(src_config['conn-str'])
    conn = driver.connect()

    yield conn
    utils.sqla_cleanup_tables(conn, src_objects)
    conn.close()
    driver.dispose()

@pytest.fixture(scope='function')
def dst_driver(src_driver):
    driver = sqlalchemy.create_engine(dst_config['conn-str'])
    conn = driver.connect()

    yield conn
    utils.sqla_cleanup_tables(conn, dst_objects)
    conn.close()
    driver.dispose()


@pytest.fixture(scope='function')
def src_engine(src_driver):
    engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine(src_config)
    yield engine
    engine.close()

@pytest.fixture(scope='function')
def dst_engine(dst_driver):
    engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine(dst_config)
    yield engine
    engine.close()


def multi_execute(driver, stmts):
    if stmts is None:
        return None
    if isinstance(stmts, str):
        return driver.execute(stmts)
    if isinstance(stmts, list) or isinstance(stmts, tuple):
        return [multi_execute(driver, x) for x in stmts]
    raise TypeError('Statements should be either str or list of str!')

def test_config(src_driver, dst_driver, src_engine, dst_engine):
    src = tests[0]['src']
    dst = tests[0]['dst']
    multi_execute(src_driver, src['setup'])
    multi_execute(dst_driver, dst['setup'])
    for c in src.get('steps', []):
        multi_execute(src_driver, c)

    dbrep.replication.full_refresh(src_engine, dst_engine,
        {'src': {'table': 'test_src'}, 'dst': {'table': 'test_dst'}})

    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')
    
    multi_execute(dst_driver, dst['cleanup'])
    multi_execute(src_driver, src['cleanup'])
    

def test_full_refresh_int_and_str(src_driver, dst_driver, src_engine, dst_engine):
    #prepare source and destination tables
    src_driver.execute("create table test_src (rid serial, col_int integer, col_str varchar(255))")
    dst_driver.execute("create table test_dst (rid integer, col_int integer, col_str varchar(255))")
    #prepare source data
    src_driver.execute("insert into test_src (col_int) values (1), (2), (3)")
    src_driver.execute("insert into test_src (col_str) values ('a'), ('b'), ('c')")

    #1 sync from scratch
    dbrep.replication.full_refresh(src_engine, dst_engine,
        {'src': {'table': 'test_src'}, 'dst': {'table': 'test_dst'}})

    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')

    #2 add to source
    src_driver.execute("insert into test_src (col_str) values ('d')")
    with pytest.raises(Exception):
        utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')

    #3 sync again
    dst_driver.execute('truncate table test_dst')
    dbrep.replication.full_refresh(src_engine, dst_engine,
        {'src': {'table': 'test_src'}, 'dst': {'table': 'test_dst'}})
    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')


def test_incremental_int_and_str(src_driver, dst_driver, src_engine, dst_engine):
    #prepare source and destination tables
    src_driver.execute("create table test_src (rid serial, col_int integer, col_str varchar(255))")
    dst_driver.execute("create table test_dst (rid integer, col_int integer, col_str varchar(255))")
    #prepare source data
    src_driver.execute("insert into test_src (col_int) values (1), (2), (3)")
    src_driver.execute("insert into test_src (col_str) values ('a'), ('b'), ('c')")
    
    #1 sync from scratch
    dbrep.replication.incremental_update(src_engine, dst_engine,
        {'src': {'table': 'test_src', 'rid': 'rid'}, 'dst': {'table': 'test_dst', 'rid': 'rid'}})
    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')
    
    #2 add to source
    src_driver.execute("insert into test_src (col_str) values ('d')")
    with pytest.raises(Exception):
        utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')
    
    #3 sync again
    dbrep.replication.incremental_update(src_engine, dst_engine,
        {'src': {'table': 'test_src', 'rid': 'rid'}, 'dst': {'table': 'test_dst', 'rid': 'rid'}})
    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')
"""