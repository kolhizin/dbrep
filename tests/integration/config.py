import os
import yaml

from errors import InvalidTestConfigError

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
    templates = read_multi_configs('template_', '.yaml', 'templates')
    return {
        'connections': connections,
        'templates': templates,
        'tests': tests
    }

def is_explicit_test(config):
    return 'test' in config and 'config' in config and 'template' not in config \
        and 'tests' not in config and 'configs' not in config

def make_explicit_tests(config):
    result = [dict(name=k, **v) for k,v in config['tests'].items() if is_explicit_test(v)]
    return result