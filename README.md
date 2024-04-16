# managed-scaling-enhanced
A tool to scale in EMR cluster more quickly.

## Getting Started

### Installation
python >= 3.6

```
pip3 install .
```
Dev environment
```
pip3 install -e .
```
### Usage
Add cluster
```
mse add-cluster --cluster-id j-xxxxx --cpu-usage-upper-bound 0.6 \
--cpu-usage-lower-bound 0.4 --cpu-usage-period-minutes 15 --cool-down-period-minutes 5
```
Check other cluster options
```
mse add-cluster --help
```

Start the scheduler
```
mse start --schedule-interval 60
```
Start the scheduler with dry run mode. This will only evaluate the cluster but not actually scale in.
```
mse start --schedule-interval 60 --dry-run
```
You can find the log in the log directory.
