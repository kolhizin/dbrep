import time
import os

import sqlalchemy
import yaml

import dbrep.utils

def flatten_test_configs(name, test):
    src = {k: test[k] for k in ['setup', 'steps', 'cleanup', 'config'] if k in test}
    return [{'src': src, 'dst': v, 'name': ''.join([name, k])} for k, v in test['dst'].items()]


def read_config():
    with open('connections.yaml', 'rb') as f:
        config = yaml.safe_load(f.read().decode('utf8'))

    test_files = [x for x in os.listdir('./') if x.startswith('test_') and (x.endswith('.yaml') or x.endswith('.yml'))]
    tests = {}
    for tf in test_files:
        with open(tf, 'rb') as f:
            tmp = yaml.safe_load(f.read().decode('utf8'))
        tests.update(tmp['tests'] if 'tests' in tmp else tmp)
    return {'tests': [x for k,v in tests.items() for x in flatten_test_configs(k,v)], 'connections': config}

def sqla_check_connection(conn_str):
    engine = sqlalchemy.create_engine(conn_str)
    try:
        engine.connect().close()
        return True
    except:
        return False

def wait_healthy(check_fn, retries=10, backoff=1):
    for i in range(retries):
        if check_fn():
            return True
        time.sleep(backoff)
    return False


def sqla_compare_outputs(out1, names1, out2, names2):
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

def sqla_compare_results(cursor1, cursor2):
    def get_output_(cur):
        names = list(cur.keys())
        out = [list(x) for x in cur.fetchall()]
        return names, out
    names1, out1 = get_output_(cursor1)
    names2, out2 = get_output_(cursor2)
    return sqla_compare_outputs(out1, names1, out2, names2)


def sqla_compare_tables(eng1, tbl1, eng2, tbl2, template='select * from {}'):
    return sqla_compare_results(eng1.execute(template.format(tbl1)), eng2.execute(template.format(tbl2)))

def sqla_cleanup_tables(engine, tables, template='drop table if exists {}'):
    for t in tables:
        engine.execute(template.format(t))