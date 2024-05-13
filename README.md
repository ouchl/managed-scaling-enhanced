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
Environment variables
```
export AWS_DEFAULT_REGION=us-west-2
export DB_CONN_STR='sqlite:///data.db'
export DB_CONN_STR='mysql+pymysql://root:mse-root@localhost:3306/mse?charset=utf8mb4'
export api_host=
```
### Usage
Add cluster
```
mse add-cluster --cluster-id j-xxxxx --cpu-usage-upper-bound 0.6 \
--cpu-usage-lower-bound 0.4 --metrics-lookback-period-minutes 15 --cool-down-period-minutes 5 --max-capacity-limit 100 \
--resize-policy CPU_BASED --scale-in-factor 1 --scale-out-factor 1
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

Reset cluster to its initial max units
```
mse reset --cluster-id j-xxxx
```
Reset all clusters
```
mse reset -a
```
