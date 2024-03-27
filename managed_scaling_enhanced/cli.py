import click

from managed_scaling_enhanced import config
from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
import json
from apscheduler.schedulers.background import BackgroundScheduler
from managed_scaling_enhanced.run import run
import time
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


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
                      cluster_group=cluster_group, configuration=config_obj.__dict__)
    session.add(cluster)
    session.commit()
    session.close()


@click.command()
def list_cluster():
    """List info of all EMR clusters."""
    session = Session()
    clusters = session.query(Cluster).all()
    clusters = [cluster.to_dict() for cluster in clusters]
    click.echo(clusters)
    session.close()


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
def delete_cluster(cluster_id):
    """Delete an EMR cluster."""
    session = Session()
    cluster = session.query(Cluster).get(cluster_id)
    if not cluster:
        click.echo(f'Cluster {cluster_id} dose not exist!')
        raise SystemExit
    session.delete(cluster)
    session.commit()
    session.close()


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
def describe_cluster(cluster_id):
    """Describe an EMR cluster by cluster id."""
    session = Session()
    cluster = session.query(Cluster).get(cluster_id)
    if not cluster:
        click.echo(f'Cluster {cluster_id} dose not exist!')
        raise SystemExit
    click.echo(json.dumps(cluster.to_dict()))
    session.close()


@click.command()
@click.option('-s', '--schedule-interval', type=click.INT, help='Schedule interval seconds of background job')
def start(schedule_interval):
    """Start background scheduled job."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(run, 'interval', seconds=schedule_interval)
    scheduler.start()
    try:
        # 主线程继续运行，直到按Ctrl+C或发生异常
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()
        click.echo("Scheduler shutdown successfully.")


cli.add_command(add, 'add-cluster')
cli.add_command(list_cluster, 'list-clusters')
cli.add_command(delete_cluster, 'delete-cluster')
cli.add_command(describe_cluster, 'describe-cluster')
cli.add_command(start, 'start')

if __name__ == '__main__':
    cli()
