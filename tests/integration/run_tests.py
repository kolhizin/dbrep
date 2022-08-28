from asyncio.log import logger
import os
import logging
logger = logging.getLogger()

import yaml
import dbrep
import dbrep.cli
from dbrep.replication import full_refresh, incremental_update

class TestDriverSQLAlchemy:
    def __init__(self, config):
        import sqlalchemy
        self.engine = sqlalchemy.create_engine(config['conn-str'])

    def execute(self, query):
        try:
            print('1-Start({}, {}): {}'.format(self, self.engine, query))
            with self.engine.connect() as conn:
                res = conn.execute(query)
                if res is not None:
                    res.close()
                conn.commit_prepared()
                conn.close()
        except:
            print('1-Fail')
        else:
            print('1-Success')

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
        return TestDriverSQLAlchemy(config)
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
    run_config = {'src': src_test['config'], 'dst': dst_test['config']}
    print('Created drivers & connections')
    try:
        logger.debug('Starting src setups')
        multi_execute(src_driver, src_test.get('setup'))
        logger.debug('Finished src setups')
        try:
            logger.debug('Finished dst setups')
            multi_execute(dst_driver, dst_test.get('setup'))
            logger.debug('Finished dst setups')


            src_engine = dbrep.cli.make_engine(src_test['config']['conn'], config)
            dst_engine = dbrep.cli.make_engine(dst_test['config']['conn'], config)
            try:
                logger.debug('Starting replication initial step')
                run_replication(mode, src_engine, dst_engine, run_config)
                logger.debug('Finished replication initial step, starting testing')
                test_replication(src_driver, dst_driver, run_config)
                logger.debug('Finished initial step testing')
                for cmd in steps:
                    logger.debug('Executing commands ({}) in src'.format(cmd))
                    multi_execute(src_driver, cmd)
                    logger.debug('Executed commands in src, starting replication')
                    run_replication(mode, src_engine, dst_engine, run_config)
                    logger.debug('Finished replication step, starting testing')
                    test_replication(src_driver, dst_driver, run_config)
                    logger.debug('Finished step testing')
            finally:
                logger.debug('Closing connections')
                src_engine.conn.close()
                dst_engine.conn.close()
                src_engine.engine.dispose()
                dst_engine.engine.dispose()
                logger.debug('Closed connections')
        finally:
            logger.debug('Starting dst cleanup')
            multi_execute(dst_driver, dst_test.get('cleanup'))
            dst_driver.engine.dispose()
            logger.debug('Finished dst cleanup')
    finally:
        logger.debug('Starting src cleanup')
        multi_execute(src_driver, src_test.get('cleanup'))
        src_driver.engine.dispose()
        logger.debug('Finished src cleanup')

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

    results = []
    for src_name, src_test in tests.items():
        for dst_name, dst_test in src_test['dst'].items():
            for mode in modes:
                try:
                    run_test(mode, src_test, dst_test, config)
                    results.append((src_name, dst_name, mode, None))
                    logger.info('Test [{}, {}, {}] succeeded'.format(src_name, dst_name, mode))
                except Exception as e:
                    results.append((src_name, dst_name, mode, e))
                    logger.info('Test [{}, {}, {}] failed: {}'.format(src_name, dst_name, mode, e))
                except:
                    print('Strange..')