from copy import deepcopy
from dataclasses import replace
import math
from types import SimpleNamespace

from analysis.teacher_bounds_20261111 import (TemporalGraph, certified_image_bound,
    image_view, no_contention_reference, service_floor)
from sim.scheduler.contact_graph import Contact, ContactPlan
from sim.scheduler.literature import ground_node
from test_dynamic_granularity_v2 import fixture, TILING


def config():
    return SimpleNamespace(dt=.1,num_sats=3,compute_service_mode='work_conserving',
        onboard_compute_profile={'service_s':{'1':2.4,'2':4.2}},
        ground_stations=[{'id':'gs','compute_rate':5}],ground_relay_delay_s=0,
        aggregation_delay_s=.01)


def test_temporal_no_hop_limit_and_no_rounding():
    records = [(i,i+1,0,100) for i in range(80)] + [(80,ground_node('gs'),.013,100)]
    value,path = TemporalGraph(records).earliest({0:0},destinations={ground_node('gs')})
    assert value == .013 and len(path) == 81


def test_future_window_not_limited_to_next_contact():
    graph = TemporalGraph([(0,1,0,1),(0,1,400,410),(1,2,401,420)])
    assert graph.earliest({0:2},destinations={2})[0] == 401
    assert ContactPlan([Contact(0,1,0,1,1,1)]).delivery(0,{1},2,1) is None


def test_zero_cycles_and_endpoint_relaxation():
    graph = TemporalGraph([(0,1,0,1),(1,0,0,1),(1,2,1,1)])
    assert graph.earliest({0:1},destinations={2})[0] == 1
    assert graph.earliest({0:2},destinations={2}) is None


def test_compute_layer_enforces_positive_service_and_ground_processing():
    ground = ground_node('gs')
    graph = TemporalGraph([(0,ground,0,.1)])
    value,path = graph.earliest({0:0},destinations={ground},service=.2,processors={0,ground})
    assert value == .2
    assert path[-1]['kind'] == 'compute' and path[-1]['source'] == ground


def test_service_floor_not_greedy_whole_block_service():
    event = {'image_tiling':TILING}
    assert service_floor(config(),event) == .2


def test_fixed_merge_and_per_window_relaxation():
    event = dict(image_tiling=TILING,release_step=20,source_sat_id=0,aggregation_delay_s=.037)
    result = certified_image_bound(TemporalGraph([(0,ground_node('gs'),0,100)]),config(),event,'task_0')
    assert math.isclose(result['latency_seconds'],.237)
    assert result['windows'] == 16
    assert result['relaxed_modes']['cooperative'] < result['relaxed_modes']['ground']


def test_more_contacts_or_lower_service_cannot_worsen():
    g = ground_node('gs')
    for service in (.1,1,5):
        a = TemporalGraph([(0,g,20,30)]).earliest({0:0},destinations={g},service=service,processors={0,g})[0]
        b = TemporalGraph([(0,g,20,30),(0,g,10,12)]).earliest({0:0},destinations={g},service=service/2,processors={0,g})[0]
        assert b <= a


def test_window_view_matches_executor_geometry_fields():
    env = fixture()
    try:
        actual = env.export_state()
        satellites = deepcopy(actual.satellites)
        satellites[0]['mem_remaining_gb'] += sum(t['data_size_gb'] for t in actual.tiles.values())
        empty = replace(actual,tiles={},pending_task_tiles={},satellites=satellites)
        event = dict(env.cfg.scheduled_task_arrivals[0],_vram_req_gb=.01)
        view = image_view(empty,event,'task_0',16)
        for tid,row in view.tiles.items():
            for key in ('data_size_mb','data_size_gb','compute_cost','unique_input_size_mb','result_size_mb','task_progress'):
                assert row[key] == actual.tiles[tid][key]
        assert view.satellites[0]['mem_remaining_gb'] == actual.satellites[0]['mem_remaining_gb']
    finally:env.close()


def test_reference_does_not_reserve_between_calls_or_leak_truth():
    env = fixture()
    try:
        state = env.export_state(); event = dict(env.cfg.scheduled_task_arrivals[0],_vram_req_gb=.01)
        a = no_contention_reference(state,event,'task_0')
        event['window_result_bytes'] = [999999]*16
        b = no_contention_reference(state,event,'task_0')
        assert a == b
        assert a['latency_seconds'] is not None
        assert len(a['blocks']['4']) == 4 and len(a['blocks']['16']) == 16
    finally:env.close()


def test_lower_bound_dominates_known_local_toy_execution():
    from sim.entities import Action, ActionType
    env = fixture()
    try:
        event = env.cfg.scheduled_task_arrivals[0]
        graph = TemporalGraph([(0,ground_node('gs'),0,1000),(1,ground_node('gs'),0,1000),(0,1,0,1000)])
        lower = certified_image_bound(graph,env.cfg,event,'task_0')
        for i in range(80):env.step([Action(t,ActionType.LOCAL) for t in env.tasks['task_0'].tile_ids] if i==0 else [])
        latency = env.tasks['task_0'].completion_time*env.cfg.dt-event['release_step']*env.cfg.dt
        assert lower['latency_seconds'] <= latency
    finally:env.close()


def test_two_layer_shortest_paths_against_bellman_ford():
    import random
    rng = random.Random(20261111)
    for _ in range(30):
        contacts = [(rng.randrange(5),rng.randrange(5),float(rng.randrange(10)),15.) for _ in range(20)]
        service = .37
        distance = {(node,phase):math.inf for node in range(5) for phase in (0,1)}
        distance[0,0] = 0
        for _ in range(10):
            before = dict(distance)
            for node in range(5): distance[node,1] = min(distance[node,1],distance[node,0]+service)
            for source,target,start,end in contacts:
                for phase in (0,1):
                    ready = max(distance[source,phase],start)
                    if ready <= end: distance[target,phase] = min(distance[target,phase],ready)
            if distance == before:break
        actual = TemporalGraph(contacts).earliest({0:0},destinations={4},service=service,processors=set(range(5)))
        assert (math.inf if actual is None else actual[0]) == distance[4,1]


def test_real_validation_geometry_small_fixture_before_publication():
    from analysis.run_dynamic_heuristics_v2 import BC,read
    from analysis.dynamic_granularity_v2 import GranularityEnv
    from analysis.teacher_bounds_20261111 import geometry_and_reference
    from sim.config import config_from_dict
    p = read(BC/'data/2026-10-22/six_hours/protocol.json')
    c = deepcopy(p['config'])
    c.update(num_sats=2,sim_steps=100,task_arrival_end_step=100,task_source_sat_ids=[0],scheduled_task_arrivals=[])
    c['topology'].update(tle_lines=c['topology']['tle_lines'][:2],neighbor_limits=[1,1],preferred_edges=[])
    env = GranularityEnv(config_from_dict(c))
    event = dict(release_step=1,source_sat_id=0,image_tiling=TILING,aggregation_delay_s=0.)
    try:
        contacts,reference,audit = geometry_and_reference(env,dict(scored_task_ids=['task_0'],
            config=dict(scheduled_task_arrivals=[event])),lambda _:None)
        assert audit['grid_samples'] == 1
        assert audit['scalar_snapshot_checks'][0]['exact_match']
        assert len(reference) == 1 and reference[0]['task_id'] == 'task_0'
    finally:env.close()
