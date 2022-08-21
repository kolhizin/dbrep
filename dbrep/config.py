"""
This module provides simple way to handle configs.
Key concept is having ability to override or generalize any given config.

So typical config structure should be as follows:
0. secret.key -- file containinig Fernet key
1. credentials.yaml.crypto -- encrypted credentials (or you may leave it unencrypted with credentials.yaml)
2. connections.yaml -- list of connections that can reference credentials {{level0.level1.level2}}
3. templates.yaml -- list of templates that are accessible for jobs
4. jobs.yaml -- list of jobs or pure job description
5. STDIN -- in form of level0.level1.level2=value, e.g. src.connection=ConName, src.table='asd'

So there must be a way to combine these multiple configs into single one. This could be done in two steps:
1. Unite all configs into single big config file: going from general to override/specific values
2. Use templating to update values of this final config
"""
import string
import functools
import copy



def make_config(list_of_pairs):
    res = {}
    for k,v in list_of_pairs:
        keys = k.split('.')
        cur = res
        for x in keys[:-1]:
            if x not in cur:
                cur[x] = {}
            cur = cur[x]
        cur[keys[-1]] = v
    return res

def config_union(*args):    
    def merge_dict(d1, d2):
        if isinstance(d1, dict) and isinstance(d2, dict):
            return {k: merge_dict(d1.get(k), d2.get(k)) for k in set.union(set(d1), set(d2))}
        return d2 or d1 #override with later        
    return functools.reduce(merge_dict, args, {})


def flatten_config(config, prefix = ''):
    if not prefix:
        formatter = '{}'
    else:
        formatter = prefix + '.{}'
    res = {formatter.format(k): v for k,v in config.items()
                    if type(v) is not dict}
    res.update({k2: v2 for k, v in config.items()
                    if type(v) is dict
                    for k2, v2 in flatten_config(v, formatter.format(k)).items()})
    return res

class TemplateDotted(string.Template):
    braceidpattern = '[_a-z][_a-z0-9\.]*'

def substitute_config(config):
    def replace_template(val, mapping):
        if isinstance(val, dict):
            return {k: replace_template(v, mapping) for k,v in val.items()}
        if isinstance(val, str):
            return TemplateDotted(val).substitute(mapping)
        return val
    flat_conf = flatten_config(config)
    while True:
        new_conf = replace_template(flat_conf, copy.deepcopy(flat_conf))
        if new_conf == flat_conf:
            break
        flat_conf = new_conf
    return replace_template(config, flat_conf)
