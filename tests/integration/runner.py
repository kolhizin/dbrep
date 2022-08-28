import copy
from distutils.log import error
import logging
import contextlib

import errors
from config import make_explicit_tests
from dbrep import create_engine, init_factory
from dbrep.replication import full_refresh, incremental_update
import dbrep.utils
from drivers import TestDriverSQLAlchemy


logger = logging.getLogger(__name__)

def validate_test_config(config, conn_config):
    if not isinstance(config, dict):
        raise errors.InvalidTestConfigError('Test config should be dict, but got {}!'.format(type(config)))
    if 'config' not in config or 'test' not in config:
        raise errors.InvalidTestConfigError('Test should contain `config` and `test` entries, but got {}'.format(config.keys()))
    if 'src' not in config['config'] or 'dst' not in config['config']:
        raise errors.InvalidTestConfigError('Test should contain src/dst in `config`-section, but got {}'.format(config['config'].keys()))
    if 'src' not in config['test'] or 'dst' not in config['test']:
        raise errors.InvalidTestConfigError('Test should contain src/dst in `test`-section, but got {}'.format(config['test'].keys()))
    if 'conn' not in config['config']['src']:
        raise errors.InvalidTestConfigError('Src-config should contain connection!')
    if 'conn' not in config['config']['dst']:
        raise errors.InvalidTestConfigError('Dst-config should contain connection!')
    if config['config']['src']['conn'] not in conn_config:
        raise errors.InvalidTestConfigError('Specified connection ({}) is absent from connection list!'.format(config['config']['src']['conn']))
    if config['config']['dst']['conn'] not in conn_config:
        raise errors.InvalidTestConfigError('Specified connection ({}) is absent from connection list!'.format(config['config']['dst']['conn']))
    return None

def make_test_driver(config):
    if 'test-driver' not in config:
        raise errors.InvalidTestConfigError('Connection config should also specify which test-driver to use, but it is missing `test-driver` field!')
    if config['test-driver'] == 'sqlalchemy':
        try:
            return TestDriverSQLAlchemy(config)
        except Exception as e:
            raise errors.InvalidTestDriverError('Failed to create sqlalchemy engine for test driver') from e
    else:
        raise errors.InvalidTestConfigError('Unexpected test-driver: {}'.format(config['test-driver']))

@contextlib.contextmanager
def test_driver(config, setup, cleanup):
    def cleanup_(drv, commands):
        excs = []
        for cmd in commands:
            try:
                drv.execute(cmd)
            except Exception as e0:
                excs.append(e0)
        drv.dispose()
        return excs

    driver = make_test_driver(config)
    setup_list = [setup] if isinstance(setup, str) else setup
    cleanup_list = [cleanup] if isinstance(cleanup, str) else cleanup
    try:
        for cmd in setup_list:
            driver.execute(cmd)
    except Exception as e:
        excs = cleanup_(driver, cleanup_list)
        raise errors.InvalidTestRuntimeError('Failed to run setup on test-driver! Gathered {} errors during cleanup.'.format(len(excs)), e, excs) from e
    try:
        yield driver
    finally:
        excs = cleanup_(driver, cleanup_list)
        if len(excs) > 0:
            raise errors.InvalidTestRuntimeError('Failed to run cleanup on test-driver! Gathered {} errors.'.format(len(excs)), excs)

@contextlib.contextmanager
def make_engine(config):
    engine = create_engine(config['engine'], config)
    try:
        yield engine
    finally:
        engine.close()

def prepare_test_stage(stage_config):
    if not isinstance(stage_config, dict):
        raise errors.InvalidTestConfigError('Stage should be dict, but got {}'.format(type(stage_config)))
    if 'steps' not in stage_config:
        return []
    steps = [stage_config['steps']] if isinstance(stage_config['steps'], str) else stage_config['steps']
    if not isinstance(steps, list):
        raise errors.InvalidTestConfigError('Steps should be of type list, but got'.format(type(steps)))
    if not all(isinstance(x, str) for x in steps):
        raise errors.InvalidTestConfigError('Steps should be list of strs, but got list of other types!')
    return steps
        
def prepare_test_stages(test_config):
    src_stages = [test_config] if 'stages' not in test_config else test_config['stages']
    if not isinstance(src_stages, list):
        raise errors.InvalidTestConfigError('Stages should be list, but got {}'.format(type(src_stages)))
    return [prepare_test_stage(x) for x in src_stages]

def run_test_stage(src_driver, dst_driver, src_engine, dst_engine, steps, config):
    try:
        for cmd in steps:
            src_driver.execute(cmd)
    except Exception as e:
        raise errors.InvalidTestRuntimeError('Failed to run steps in one of test stages!', e) from e

    found_handler = True
    try:
        if config['mode'] == 'full-refresh':
            full_refresh(src_engine, dst_engine, config)
        elif config['mode'] == 'incremental':
            incremental_update(src_engine, dst_engine, config)
        else:
            found_handler = False
    except Exception as e:
        raise errors.FailedReplicationError('Failed to run replication!', e) from e
    if not found_handler:
        raise errors.InvalidTestConfigError("Unsupported mode: {}. Should be full-refresh or incremental".format(config['mode']))

def test_replication(src_keys, src_data, dst_keys, dst_data):
    try:
        if src_keys != dst_keys:
            raise Exception('Names should be the same, but got {} != {}!'.format(src_keys, dst_keys))
        dbrep.utils.test_output_type(src_data)
        dbrep.utils.test_output_type(dst_data)
        dbrep.utils.test_unique_key(src_data)
        dbrep.utils.test_unique_key(dst_data)
        output = dbrep.utils.merge_outputs(src_data, dst_data)
    except Exception as e:
        raise errors.BadReplicationError('Problems with table types/outputs after replication', e) from e
    tests = [
        dbrep.utils.test_elem_typing,
        dbrep.utils.test_elem_none,
        dbrep.utils.test_elem_type,
        dbrep.utils.test_elem_value,
    ]
    result = []
    for test in tests:
        try:
            dbrep.utils.run_tests(output, test, report_cols=True)
        except Exception as e:
            result.append(e)
        else:
            result.append(None)
    return result

def run_test(test_config, conn_config):
    logger.debug('Starting test')
    validate_test_config(test_config, conn_config)

    src_conn_config = conn_config[test_config['config']['src']['conn']]
    dst_conn_config = conn_config[test_config['config']['dst']['conn']]
    src_test = test_config['test']['src']
    dst_test = test_config['test']['dst']

    stages = prepare_test_stages(test_config['test']['src'])
    results = []

    with test_driver(src_conn_config, src_test.get('setup', []), src_test.get('cleanup', [])) as src_driver:
        with test_driver(dst_conn_config, dst_test.get('setup', []), dst_test.get('cleanup', [])) as dst_driver:
            for steps in stages:
                with make_engine(src_conn_config) as src_engine:
                    with make_engine(dst_conn_config) as dst_engine:
                        run_test_stage(src_driver, dst_driver, src_engine, dst_engine, steps, test_config['config'])
                try:
                    src_keys, src_data = src_driver.fetchall(test_config['config']['src'])
                except Exception as e:
                    raise errors.InvalidTestRuntimeError('Failed to query src-data to test replication correctness', e) from e
                try:
                    dst_keys, dst_data = dst_driver.fetchall(test_config['config']['dst'])
                except Exception as e:
                    raise errors.InvalidTestRuntimeError('Failed to query dst-data to test replication correctness', e) from e
                results.append(test_replication(src_keys, src_data, dst_keys, dst_data))
    return results


def run_tests(config):
    init_factory()
    tests = make_explicit_tests(config)
    result = []
    for test in tests:
        tmp = copy.deepcopy(test)
        try:
            tmp['result'] = run_test(test, config['connections'])
        except errors.InvalidTestError as e:
            tmp['result'] = {'status': 'invalid-test', 'error': e}
        except errors.ReplicationError as e:
            tmp['result'] = {'status': 'failed-replication', 'error': e}
        except Exception as e:
            tmp['result'] = {'status': 'unexpected', 'error': e}
        print(tmp['result'])
        result.append(tmp)

    return result
