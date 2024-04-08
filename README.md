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
mse add-cluster --cluster-id j-xxxxx
```
Check other cluster options
```
mse add-cluster --help
```

Start the scheduler
```
mse start --schedule-interval 60
```


