"""仅替换完整图像协同候选的块级规划，保留旧选择/报价/承诺路径。"""
from types import FunctionType

from analysis.dynamic_granularity_v2 import DynamicThreePlan
from sim.scheduler.literature import HEFTAdapted


def heft_with_plan(state, plan):
    original = HEFTAdapted.select_actions
    namespace = dict(original.__globals__, contact_plan=lambda _: plan)
    bound = FunctionType(original.__code__, namespace, original.__name__,
                         original.__defaults__, original.__closure__)
    return bound(HEFTAdapted(), state)


class HeftCooperativeThreePlan(DynamicThreePlan):
    def _select_actions_with_plan(self, state, plan):
        tasks = {row['parent_task_id'] for row in state.tiles.values()}
        if len(tasks) == 1 and not (tasks & self.decided) and all(
                row['state'] == 'QUEUED' and not row['in_transfer']
                and not row['task_progress']['failed'] and row['location'] == row['source_sat_id']
                for row in state.tiles.values()):
            # preview的4/16块数不同于实体旧布局，用task_pending_count识别完整候选。
            if all(row['task_pending_count'] == len(state.tiles) for row in state.tiles.values()):
                return heft_with_plan(state, plan)
        return super()._select_actions_with_plan(state, plan)
