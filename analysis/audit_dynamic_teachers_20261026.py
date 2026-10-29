"""从承诺位置、窗口权重与掩码独立复算教师覆盖。"""
from collections import Counter
import math
from analysis import run_dynamic_teachers as teacher
from analysis.redesign_scenario import write_json


def audit():
    m = teacher.verify_manifest()
    root = teacher.ROOT
    assert teacher.sha(root/'manifest.json') == '7ab185640422280d1ac39bd5df887f630aea11bc4f44cc95f10f8b7edb0d4eb5'
    assert teacher.read(root/'final_status.json')['status'] == 'COMPLETE'
    for path, h in teacher.read(root/'FINAL_AUDIT.json')['hashes'].items():
        assert teacher.sha(path) == h, path
    first = root/'episodes'/teacher.episode(m['train'][0])
    a, b = [teacher.read(first/f'{role}.json') for role in ('reference', 'teacher')]
    assert all(a[k] == b[k] for k in ('outcomes', 'dynamics', 'action_trace_sha256'))
    totals = Counter(); modes = Counter(); images = illegal = 0; files = {}
    helper_total = helper_inside = 0.
    categories = ('represented_windows', 'outside_topk_windows', 'masked_windows',
                  'uncommitted_windows', 'unsupported_windows')
    for path in m['train']:
        p = teacher.read(path); teacher.verify(p)
        f = root/'episodes'/teacher.episode(path)/'teacher.json'
        r = teacher.read(f); files[str(f)] = teacher.sha(f)
        assert r['status'] == 'PASS' and r['checks']['passed']
        expected = {e['task_id']:e['compute_windows'] for e in p['events'] if e['scored']}
        assert set(expected) == set(p['scored_task_ids'])
        outcomes = {o['task_id']:o for o in r['outcomes']}
        choices = {c['task_id']:c['selected'] for c in r['dynamics']['choices']}
        for task, windows in expected.items():
            images += 1; totals['total_windows'] += windows
            assert outcomes[task]['observed_age_seconds'] >= 600
            if task not in r['labels']:
                totals['uncaptured_windows'] += windows; continue
            row = r['labels'][task]; obs = row['observation']; label = row['label']
            selected = choices[task]
            mode = 2 if selected == 'ground' else 0 if selected.endswith('_4') else 1
            assert mode == label['mode']; modes[selected] += 1
            v = obs['variants'][min(mode, 1)]
            assert math.isclose(sum(row['window_weights'].values()), windows)
            counts = Counter(); mass = [0.]*len(v['node_ids']); ground = 0.
            illegal += not obs['image_mask'][mode]
            if mode == 2:
                assert all(x == 'raw_ground' for x in row['actual_commitments'].values())
                counts['represented_windows' if obs['image_mask'][mode] else 'masked_windows'] = windows
            else:
                for tid, w in row['window_weights'].items():
                    target = row['actual_commitments'].get(tid)
                    if isinstance(target, int) and target != obs['source']:
                        helper_total += w
                        helper_inside += w if target in v['node_ids'] else 0
                    if target is None: category = 'uncommitted_windows'
                    elif target not in v['node_ids']:
                        category = 'outside_topk_windows' if isinstance(target, int) else 'unsupported_windows'
                    elif not v['position_mask'][v['node_ids'].index(target)]: category = 'masked_windows'
                    else:
                        category = 'represented_windows'; mass[v['node_ids'].index(target)] += w
                        if target == 'ground_block' and obs['image_mask'][mode]: ground += w
                    counts[category] += w
            for key in categories:
                assert counts[key] == label[key], (task, key)
                totals[key] += counts[key]
            assert ground == label['ground_block_windows']
            totals['ground_block_windows'] += ground
            soft = [x/counts['represented_windows'] if counts['represented_windows'] and mode < 2 else 0. for x in mass]
            assert all(math.isclose(x,y,abs_tol=1e-12) for x,y in zip(soft,label['soft_positions']))
            assert sum(counts.values()) == windows
    assert sum(totals[k] for k in (*categories, 'uncaptured_windows')) == totals['total_windows']
    recorded = teacher.read(root/'TEACHER_COVERAGE.json')
    assert images == recorded['images'] and illegal == recorded['illegal_image_mask_images']
    assert all(totals[k] == v for k,v in recorded['totals'].items())
    result = dict(status='PASS', images=images, totals=dict(totals), selected=dict(modes),
        represented_fraction=totals['represented_windows']/totals['total_windows'],
        helper_window_topk_coverage=helper_inside/helper_total if helper_total else None,
        helper_windows=helper_total, illegal_image_mask_images=illegal, teacher_hashes=files,
        note='Ground blocks are a represented subset; all scored windows form the denominator.',
        tests_sealed=True)
    out = root/'independent_audit_20261026'
    assert not out.exists()
    write_json(out/'AUDIT.json', result)
    print(result)


if __name__ == '__main__': audit()
