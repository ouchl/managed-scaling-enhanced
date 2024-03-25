import click

import config
from database import engine, Session
from models import Cluster
import json


@click.group()
def cli():
    pass


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
@click.option('--cluster-name', default=None, help='EMR cluster name')
@click.option('--cluster-group', default=None, help='EMR cluster group')
@click.option('--configuration', default='{}', help='EMR cluster configuration')
def add(cluster_id, cluster_name, cluster_group, configuration):
    """Add an EMR cluster to be managed by this tool."""
    session = Session()
    config_obj = config.Config(**json.loads(configuration))
    cluster = Cluster(id=cluster_id, cluster_name=cluster_name,
                      cluster_group=cluster_group, configuration=json.dumps(config_obj.__dict__))
    session.add(cluster)
    session.commit()


@click.command()
def list_cluster():
    """List info of all EMR clusters."""
    session = Session()
    clusters = session.query(Cluster).all()
    clusters = [cluster.to_dict() for cluster in clusters]
    click.echo(json.dumps(clusters))


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
def delete_cluster(cluster_id):
    """Delete a EMR cluster."""
    session = Session()
    cluster = session.query(Cluster).get(cluster_id)
    if not cluster:
        click.echo(f'Cluster {cluster_id} dose not exist!')
        raise SystemExit
    session.delete(cluster)
    session.commit()


cli.add_command(add, 'add-cluster')
cli.add_command(list_cluster, 'list-cluster')
cli.add_command(delete_cluster, 'delete-cluster')

if __name__ == '__main__':
    cli()
