from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Config:
    minimumUnits: Decimal = 300
    maximumUnits: Decimal = 1000
    spotInstancesTimeout: Decimal = 60*30
    scaleOutAvgYARNMemoryAvailablePercentageValue: Decimal = 30
    scaleOutAvgYARNMemoryAvailablePercentageMinutes: Decimal = 5
    scaleOutAvgCapacityRemainingGBValue: Decimal = 256
    scaleOutAvgCapacityRemainingGBMinutes: Decimal = 5
    scaleOutAvgPendingAppNumValue: Decimal = 3
    scaleOutAvgPendingAppNumMinutes: Decimal = 5
    scaleOutAvgTaskNodeCPULoadValue: Decimal = 42
    scaleOutAvgTaskNodeCPULoadMinutes: Decimal = 15

    scaleInAvgYARNMemoryAvailablePercentageValue: Decimal = 40
    scaleInAvgYARNMemoryAvailablePercentageMinutes: Decimal = 3
    scaleInAvgCapacityRemainingGBValue: Decimal = 5120
    scaleInAvgCapacityRemainingGBMinutes: Decimal = 3
    scaleInAvgPendingAppNumValue: Decimal = 2
    scaleInAvgPendingAppNumMinutes: Decimal = 2
    scaleInAvgTaskNodeCPULoadValue: Decimal = 30
    scaleInAvgTaskNodeCPULoadMinutes: Decimal = 15

    scaleOutFactor: Decimal = 1.5
    scaleInFactor: Decimal = 1.7

    scaleOutCooldownSeconds: Decimal = 60*7
    scaleInCooldownSeconds: Decimal = 60*5
    maximumOnDemandInstancesNumValue: int = 160
