import os
import yaml
import dbrep
import dbrep.cli
from dbrep.replication import full_refresh, incremental_update

def multi_execute(driver, stmts):
    if stmts is None:
        return None
    if isinstance(stmts, str):
        return driver.execute(stmts)
    if isinstance(stmts, list) or isinstance(stmts, tuple):
        return [multi_execute(driver, x) for x in stmts]
    raise TypeError('Statements should be either str or list of str!')

def create_driver(config):
    if config['type'] == 'sqlalchemy':
        import sqlalchemy
        return sqlalchemy.create_engine(config['conn-str'])
    raise ValueError('Unexpected type of test driver: {}'.format(config['type']))
    return None

modes = ['full-refresh', 'incremental']

def run_replication(mode, src_engine, dst_engine, run_config):
    if mode == 'full-refresh':
        return full_refresh(src_engine, dst_engine, run_config)
    elif mode == 'incremental':
        return incremental_update(src_engine, dst_engine, run_config)
    else:
        raise ValueError("Unsupported mode: {}. Should be full-refresh or incremental".format(mode))

def test_replication(src_driver, dst_driver, config):
    pass

def run_test(mode, src_test, dst_test, config):
    steps = src_test['steps']
    if isinstance(steps, str):
        steps = [steps]
    if not isinstance(steps, list) or not all(isinstance(x, str) for x in steps):
        raise TypeError('Steps should be list of str!')
    print('Creating drivers & connections')
    src_driver = create_driver(drivers[src_test['driver']])
    dst_driver = create_driver(drivers[dst_test['driver']])
    src_engine = dbrep.cli.make_engine(src_test['config']['conn'], config)
    dst_engine = dbrep.cli.make_engine(dst_test['config']['conn'], config)
    run_config = {'src': src_test['config'], 'dst': dst_test['config']}
    print('Created drivers & connections')
    try:
        multi_execute(src_driver, src_test.get('setup'))
        try:
            multi_execute(dst_driver, dst_test.get('setup'))
            run_replication(mode, src_engine, dst_engine, run_config)
            test_replication(src_driver, dst_driver, run_config)
            for cmd in steps:
                src_driver.execute(cmd)
                run_replication(mode, src_engine, dst_engine, run_config)
                test_replication(src_driver, dst_driver, run_config)
        finally:
                multi_execute(dst_driver, dst_test.get('cleanup'))
    finally:
        multi_execute(src_driver, src_test.get('cleanup'))

if __name__ == '__main__':
    dbrep.cli.init_factory()
    with open('connections.yaml', 'rb') as f:
        config = yaml.safe_load(f.read().decode('utf8'))
    drivers = config['drivers']
    connections = config['connections']

    test_files = [x for x in os.listdir('./') if x.startswith('test_') and (x.endswith('.yaml') or x.endswith('.yml'))]
    tests = {}
    for tf in test_files:
        with open(tf, 'rb') as f:
            tmp = yaml.safe_load(f.read().decode('utf8'))
        tests.update(tmp['tests'] if 'tests' in tmp else tmp)

    for src_name, src_test in tests.items():
        for dst_name, dst_test in src_test['dst'].items():
            for mode in modes:
                run_test(mode, src_test, dst_test, config)