from enum import Enum
class NumaScheme(Enum):
    OS_CONTROLLED = 1
    BIND_TO_NUMA = 2
    BIND_TO_CORE = 3