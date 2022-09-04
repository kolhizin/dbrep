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
from typing import Any, Dict, List, Tuple


def make_config(list_of_pairs : List[Tuple[str, Any]]) -> dict:
    """
    Convert list of pairs into structured config/dictionary.
    E.g. convert (a.b.c, 3) into {'a': {'b': {'c': 3}}}
    """
    if not list_of_pairs: #None, empty list and other should evaluate to empty dict
        return {}
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

def merge_config(*args : dict) -> dict:
    """
    Merge several configs into single one. Later argument overrides the former.
    """
    def merge_dict(d1, d2):
        if isinstance(d1, dict) and isinstance(d2, dict):
            return {k: merge_dict(d1.get(k), d2.get(k)) for k in set.union(set(d1), set(d2))}
        return d2 or d1 #override with later        
    return functools.reduce(merge_dict, args, {})


def flatten_config(config : dict, prefix : str = '') -> dict:
    """
    Convert config into flat map like a.b.c -> 1, a.b.d ->2, etc.
    """
    if not config: #None, empty dict and other should evaluate to empty dict
        return {}
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

def instantiate_templates(config: dict, templates: dict, keywords = ['template', 'templates']) -> dict:
    """
    Insert template values for `templates` dictionary into config, e.g.
    {
        "templates": ["a", "b"],
        "val": 3
    }
    {
        "a": {"va": 4, "vb": 5},
        "b": {"vb": 7, "vc": 8}
    }
    Should return
    {
        "templates": ["a", "b"],
        "va": 4,
        "vb": 7,
        "vc": 8,
        "val": 3
    }
    """
    if not config:
        return config
    if not templates:
        return config
    templates_to_use = []
    for k in keywords:
        if k in config:
            if isinstance(config[k], str):
                templates_to_use.append(config[k])
            elif isinstance(config[k], list) or isinstance(config[k], tuple):
                if not all(isinstance(x, str) for x in config[k]):
                    raise TypeError('Template value should be list of strings or single string, but got list with inconsistent types')
                templates_to_use += list(config[k])
            else:
                raise TypeError('Template value should be list of strings or single string, but got {}'.format(type(config[k])))
    if not templates_to_use: #exit early
        return config

    for t in templates_to_use:
        if t not in templates:
            raise ValueError('Template `{}` not present in templates dictionary'.format(t))
    
    result = merge_config(config, *[templates[t] for t in templates_to_use])
    return {k: instantiate_templates(k, templates, keywords=keywords) for k,v in result.items()}
        

class TemplateDotted(string.Template):
    braceidpattern = r'[_a-z][_a-z0-9\.@\-]*'

def substitute_config(config : dict) -> dict:
    """
    Replace values like ${a.b.c} with value of a.b.c from this config
    """
    def replace_template(val, mapping):
        if isinstance(val, dict):
            return {k: replace_template(v, mapping) for k,v in val.items()}
        if isinstance(val, str):
            return TemplateDotted(val).substitute(mapping)
        return val
        
    if not config: #None, empty dict and other should evaluate to empty dict
        return {}
    flat_conf = flatten_config(config)
    while True:
        new_conf = replace_template(flat_conf, copy.deepcopy(flat_conf))
        if new_conf == flat_conf:
            break
        flat_conf = new_conf
    return replace_template(config, flat_conf)
