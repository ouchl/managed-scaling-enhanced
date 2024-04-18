from pathlib import Path
import json
import boto3


def get_ec2_types():
    ec2_client = boto3.client('ec2')
    cache_path = Path('ec2_types.json')
    if cache_path.exists():
        with open(cache_path, 'r') as f:
            ec2_type_cpu_map = json.load(f)
    else:
        ec2_type_cpu_map = {}
        paginator = ec2_client.get_paginator('describe_instance_types')
        page_iterator = paginator.paginate()

        for page in page_iterator:
            for instance_type in page['InstanceTypes']:
                ec2_type_cpu_map[instance_type['InstanceType']] = instance_type['VCpuInfo']['DefaultVCpus']
        with open(cache_path, 'w') as f:
            json.dump(ec2_type_cpu_map, f)
    return ec2_type_cpu_map


ec2_types = get_ec2_types()
