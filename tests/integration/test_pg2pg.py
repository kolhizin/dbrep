from distutils.command import check
import functools
import time

import pytest
import sqlalchemy

import dbrep.engines.engine_sqlalchemy
import dbrep.utils
import dbrep.replication
import utils

conn_str = 'postgresql+psycopg2://postgres:postgres@test-db-postgres:5432/postgres'
src_conn_str = conn_str
dst_conn_str = conn_str


#@pytest.fixture(scope='function')
def engines():
    if not utils.wait_healthy(functools.partial(utils.sqla_check_connection, src_conn_str)):
        raise ConnectionError('Could not connect to DataBase')
    if not utils.wait_healthy(functools.partial(utils.sqla_check_connection, dst_conn_str)):
        raise ConnectionError('Could not connect to DataBase')

    src_driver = sqlalchemy.create_engine(src_conn_str)
    dst_driver = sqlalchemy.create_engine(dst_conn_str)

    return src_driver, dst_driver


def test_full_refresh_int_and_str():
    src_driver, dst_driver = engines()
    utils.sqla_cleanup_tables(src_driver, ['test_src'])
    utils.sqla_cleanup_tables(dst_driver, ['test_dst'])

    src_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': src_conn_str})
    dst_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': dst_conn_str})
    
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
    dbrep.replication.full_refresh(src_engine, dst_engine,
        {'src': {'table': 'test_src'}, 'dst': {'table': 'test_dst'}})
    utils.sqla_compare_tables(src_driver, 'test_src', dst_driver, 'test_dst')


def test_incremental_int_and_str():
    src_driver, dst_driver = engines()
    utils.sqla_cleanup_tables(src_driver, ['test_src'])
    utils.sqla_cleanup_tables(dst_driver, ['test_dst'])

    src_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': src_conn_str})
    dst_engine = dbrep.engines.engine_sqlalchemy.SQLAlchemyEngine({'conn-str': dst_conn_str})
    
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