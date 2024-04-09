import click

from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
from apscheduler.schedulers.background import BackgroundScheduler
from managed_scaling_enhanced.run import run
import time


@click.group()
def cli():
    pass


@click.command()
@click.option('--cluster-id', required=True, help='EMR cluster ID')
@click.option('--cluster-name', default=None, help='EMR cluster name')
@click.option('--cluster-group', default=None, help='EMR cluster group')
@click.option('--cpu-usage-upper-bound', default=0.6)
@click.option('--cpu-usage-lower-bound', default=0.4)
@click.option('--cpu-usage-period-minutes', default=15)
@click.option('--cool-down-period-minutes', default=5)
def add(cluster_id, cluster_name, cluster_group, cpu_usage_upper_bound, cpu_usage_lower_bound,
        cpu_usage_period_minutes, cool_down_period_minutes):
    """Add an EMR cluster to be managed by this tool."""
    session = Session()
    cluster = Cluster(id=cluster_id, cluster_name=cluster_name,
                      cluster_group=cluster_group, cpu_usage_upper_bound=cpu_usage_upper_bound,
                      cpu_usage_lower_bound=cpu_usage_lower_bound, cpu_usage_period_minutes=cpu_usage_period_minutes,
                      cool_down_period_minutes=cool_down_period_minutes)
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
    click.echo(cluster.to_dict())
    session.close()


@click.command()
@click.option('-s', '--schedule-interval',
              required=True, type=click.INT, help='Schedule interval seconds of background job')
@click.option('--dry-run', is_flag=True, help='Dry run mode')
def start(schedule_interval, dry_run):
    """Start background scheduled job."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(run, 'interval', args=[dry_run], seconds=schedule_interval)
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
