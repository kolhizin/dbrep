from distutils.log import error
import logging

import errors

logger = logging.getLogger(__name__)

def validate_test_config(config):
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
    return None

def run_test(test_config, conn_config):
    validate_test_config(test_config)
    if not isinstance(test_config, dict):
        raise errors.InvalidTestConfigError('Test config should be dict, but got {}')
    logger.debug('Starting test')