#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Trick, the value should be the same as PASS in html_report.py
PASS = "Pass"
CASE_STAT = 'stats'


def reorder(case, results):
    if case.lower() != CASE_STAT:
        return results

    results.sort(cmp=_compare_stat)
    results = map(_format_stat, results)

    return results


def unify(case, ref, out):
    if case.lower() != CASE_STAT:
        return

    _unify_stat(ref)
    _unify_stat(out)


def _unify_stat(stat):
    for k in stat.keys():
        stat[_unify_stat_name(k)] = stat.pop(k)


# The field name is updated, that's is for compatibility.
def _unify_stat_name(field_name):
    unified_name_dic = {'link': 'way',
                        'nav_link': 'nav_link[way]',
                        'hov_link': 'hov_link[way]',
                        }

    return unified_name_dic.get(field_name, field_name)


def _stat_key(x):
    """x: [field_name,ref_value,out_value,condition,status]

    field name example:
    node
    node[node]
    ocean[relation->natural]
    """
    field_name, status = x[0], x[4]
    parts = field_name.split('[', 1)

    feature = parts[0]
    typ = ''
    if len(parts) == 2:
        typ = parts[1].strip(']')

    return status, typ, feature


def _compare_stat(x, y):
    return cmp(_stat_key(x), _stat_key(y))


def _format_stat(x):
    status, typ, feature = _stat_key(x)

    field_name, ref_value, out_value, condition, status = x
    field_name = ':'.join(filter(None, typ.split('->') + [feature]))
    dif = _diff_rate(ref_value, out_value, status)

    return field_name, ref_value, out_value, dif, status


def _diff_rate(ref_value, out_value, status):
    if status == PASS:
        return '=='

    ref_value = 0 if not ref_value else int(ref_value)
    out_value = 0 if not out_value else int(out_value)

    infinity = '∞'
    dif_value = out_value - ref_value

    if not ref_value:
        return '%s' % infinity
    else:
        return '%.2f%%' % (float(dif_value)*100/ref_value)

