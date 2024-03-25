import boto3

# Create an EMR client
emr_client = boto3.client('emr', region_name='cn-north-1')

# Specify the cluster ID of the EMR cluster you want to get information about
cluster_id = 'j-3UQL95LORK0Q9'

# Retrieve information about the EMR cluster
response = emr_client.describe_cluster(ClusterId=cluster_id)

# Print the cluster information
print(response['Cluster'])
