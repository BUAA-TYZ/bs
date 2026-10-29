"""跨平台快照逐字段差异诊断，不修改录制或计时结果。"""
import gzip
import pickle
from analysis.method_cost_20261028 import ROOT, read, rebuild, write


def differences(a, b, path=''):
    if isinstance(a, dict) and isinstance(b, dict):
        assert a.keys() == b.keys(), path
        return sum((differences(a[k], b[k], f'{path}/{k}') for k in a), [])
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        assert len(a) == len(b), path
        return sum((differences(x, y, f'{path}/{i}') for i, (x, y) in enumerate(zip(a, b))), [])
    if a == b:
        return []
    return [dict(path=path, rebuilt=a, recorded=b,
                 absolute_error=abs(a-b) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None)]


def main():
    result = {}
    for family in ('fixed', 'dynamic'):
        rows = []
        for filename in read(ROOT/'capture'/family/'result.json')['decision_hashes']:
            with gzip.open(filename, 'rb') as stream:
                row = pickle.load(stream)
            delta = differences(rebuild(row['snapshot'], family), row['observation'])
            if delta:
                rows.append(dict(file=filename, differences=delta))
        result[family] = rows
    write(ROOT/'orin_snapshot_differences.json', result)
    for family, rows in result.items():
        print(family, 'differing records', len(rows), 'fields', sum(len(r['differences']) for r in rows))
        print(rows[:1])


if __name__ == '__main__':
    main()
