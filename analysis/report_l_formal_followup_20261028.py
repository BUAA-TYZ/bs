"""正式协议在生成后冻结于run_manifest，向既有报告器补充其路径。"""
from analysis import audit_l_reduced_20261028 as audit
from analysis.run_dynamic_heuristics_v2 import read


def main():
    audit.ROOT = audit.ROOT.parent/'l_formal_test_20261028'
    audit.OUT = audit.ROOT/'independent_audit_20261028'
    manifest_path = audit.ROOT/'manifest.json'
    run = read(audit.ROOT/'run_manifest.json')

    def report_read(path):
        value = read(path)
        if path == manifest_path:
            # 仅补运行时生成的协议清单，不改磁盘manifest或其hash校验。
            value = {**value, 'protocols':run['protocols']}
        return value

    audit.read = report_read
    audit.main(formal=True)


if __name__ == '__main__':
    main()
