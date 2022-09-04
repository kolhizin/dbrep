import os
import yaml

from errors import InvalidTestConfigError
import dbrep.config

def read_multi_configs(prefix, postfix, names):
    files = [x for x in os.listdir('./')
                if x.startswith(prefix or '') and x.endswith(postfix or '')]
    res = {}
    for fn in files:
        with open(fn, 'rb') as f:
            tmp = yaml.safe_load(f.read().decode('utf8'))
        res.update(tmp[names] if names in tmp else tmp)
    return res

def read_test_configs():
    with open('connections.yaml', 'rb') as f:
        connections = yaml.safe_load(f.read().decode('utf8'))

    tests = read_multi_configs('test_', '.yaml', 'tests')
    templates = dbrep.config.unflatten_config(read_multi_configs('template_', '.yaml', 'templates'))
    print('----------------')
    print(templates)
    print('----------------')
    return dbrep.config.unflatten_config({
        'connections': connections,
        'templates': templates,
        'tests': tests
    })

def is_explicit_test(config):
    return 'test' in config and 'config' in config and 'template' not in config \
        and 'tests' not in config and 'configs' not in config

def make_explicit_tests(config):
    def expand_test_config(cfg):
        res = dbrep.config.expand_config(cfg, 'configs', 'config')
        res = [v for x in res for v in dbrep.config.expand_config(x, 'config.src.conns', 'config.src.conn')]
        res = [v for x in res for v in dbrep.config.expand_config(x, 'config.dst.conns', 'config.dst.conn')]
        res = [v for x in res for v in dbrep.config.expand_config(x, 'tests', 'test')]
        res = [v for x in res for v in dbrep.config.expand_config(x, 'modes', 'mode')]
        return res
    templates = config.get('templates', [])
    print('template=',templates)
    res = [dbrep.config.instantiate_templates(x, templates) for x in config['tests']]
    return [v for x in res for v in expand_test_config(x)]