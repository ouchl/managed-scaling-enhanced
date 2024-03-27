export AWS_DEFAULT_REGION=cn-north-1
source venv/bin/activate
mse add-cluster --cluster-id j-xxxxx --configuration \
'{
  "minimumUnits": 300,
  "maximumUnits": 1000,
  "maximumOnDemandUnits": 160,
  "spotSwitchOnDemand": false,
  "spotInstancesTimeout": 1800,
  "scaleOutAvgYARNMemoryAvailablePercentageValue": 30,
  "scaleOutAvgYARNMemoryAvailablePercentageMinutes": 5,
  "scaleOutAvgCapacityRemainingGBValue": 256,
  "scaleOutAvgCapacityRemainingGBMinutes": 5,
  "scaleOutAvgPendingAppNumValue": 3,
  "scaleOutAvgPendingAppNumMinutes": 5,
  "scaleOutAvgTaskNodeCPULoadValue": 42,
  "scaleOutAvgTaskNodeCPULoadMinutes": 15,
  "scaleInAvgYARNMemoryAvailablePercentageValue": 40,
  "scaleInAvgYARNMemoryAvailablePercentageMinutes": 3,
  "scaleInAvgCapacityRemainingGBValue": 5120,
  "scaleInAvgCapacityRemainingGBMinutes": 3,
  "scaleInAvgPendingAppNumValue": 2,
  "scaleInAvgPendingAppNumMinutes": 2,
  "scaleInAvgTaskNodeCPULoadValue": 30,
  "scaleInAvgTaskNodeCPULoadMinutes": 15,
  "scaleOutFactor": 1.5,
  "scaleInFactor": 1.7,
  "scaleOutCooldownSeconds": 420,
  "scaleInCooldownSeconds": 300
}'

