from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from sim.entities import Action, EnvState


class SchedulerPolicy(ABC):
    @abstractmethod
    def select_actions(self, env_state: EnvState) -> List[Action]:
        raise NotImplementedError
