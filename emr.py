import boto3

# Create an EMR client
emr_client = boto3.client('emr')

# Specify the cluster ID of the EMR cluster you want to get information about
cluster_id = 'j-1SJOW088JSHLK'

# Retrieve information about the EMR cluster
response = emr_client.describe_cluster(ClusterId=cluster_id)
print(response['Cluster'])
response = emr_client.get_managed_scaling_policy(ClusterId=cluster_id)
# scaling_policies = response.get('ScalingPolicies', [])
print(response['ManagedScalingPolicy']['ComputeLimits'])
# Print the cluster information
