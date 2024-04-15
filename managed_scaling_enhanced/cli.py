import click

from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
from apscheduler.schedulers.background import BackgroundScheduler
from managed_scaling_enhanced.run import run
import time
import boto3
import random

emr_client = boto3.client('emr')


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
    cluster.initial_managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)['ManagedScalingPolicy']
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
@click.option('--cluster-id', required=True, help='EMR cluster ID')
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
@click.option('--cluster-id', required=True, help='EMR cluster ID')
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
@click.option('-s', '--schedule-interval', type=click.INT, help='Schedule interval seconds of background job')
@click.option('--dry-run', is_flag=True, help='Dry run mode')
@click.option('--run-once', is_flag=True, help='Run only once')
@click.option('--event-queue', help='EMR event queue name')
def start(schedule_interval, run_once, dry_run, event_queue):
    """Start background scheduled job."""
    if run_once:
        run(dry_run, event_queue)
    else:
        scheduler = BackgroundScheduler()
        scheduler.add_job(run, 'interval', args=[dry_run, event_queue], seconds=schedule_interval)
        scheduler.start()
        try:
            # 主线程继续运行，直到按Ctrl+C或发生异常
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # 关闭调度器
            scheduler.shutdown()
            click.echo("Scheduler shutdown successfully.")


@click.command()
@click.option('--cluster-id', required=True, help='EMR cluster ID')
@click.option('--job-number', default=1, help='Number of jobs to run')
def run_test_job(cluster_id, job_number):
    """Run test job."""
    step = {
        'Name': 'Pi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--executor-memory', '1G',
                '--num-executors', '2',
                '--executor-cores', '1',
                "--conf", "spark.dynamicAllocation.enabled=false",
                '--class', 'org.apache.spark.examples.SparkPi',
                '/usr/lib/spark/examples/jars/spark-examples.jar',
                '1000000'
            ]
        }
    }
    client = boto3.client('emr')
    for _ in range(job_number):
        client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )


@click.command()
@click.option('--cluster-id', required=True, help='EMR cluster ID')
@click.option('--job-number', default=1, help='Number of jobs to kill')
def kill_test_job(cluster_id, job_number):
    """Kill test job."""
    with Session() as session:
        cluster: Cluster = session.get(Cluster, cluster_id)
        running_apps = cluster.list_running_apps()
        if len(running_apps) > job_number:
            terminating_apps = random.sample(running_apps, job_number)
        else:
            terminating_apps = running_apps
        for app_id in terminating_apps:
            click.echo(f'Killing {app_id}')
            click.echo(cluster.kill_app(app_id).text)


cli.add_command(add, 'add-cluster')
cli.add_command(list_cluster, 'list-clusters')
cli.add_command(delete_cluster, 'delete-cluster')
cli.add_command(describe_cluster, 'describe-cluster')
cli.add_command(run_test_job, 'run-test-job')
cli.add_command(kill_test_job, 'kill-test-job')
cli.add_command(start, 'start')

if __name__ == '__main__':
    cli()
