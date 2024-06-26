import click

from managed_scaling_enhanced import boto3_config
from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster, ResizePolicy
from apscheduler.schedulers.background import BackgroundScheduler
from managed_scaling_enhanced.run import run
import time
import boto3
import random
from tabulate import tabulate

emr_client = boto3.client('emr', config=boto3_config)


@click.group()
def cli():
    pass


@click.command()
@click.option('--cluster-id', required=True, help='EMR cluster ID')
@click.option('--cluster-name', default=None, help='EMR cluster name')
@click.option('--cluster-group', default=None, help='EMR cluster group')
@click.option('--cpu-usage-upper-bound', default=0.6)
@click.option('--cpu-usage-lower-bound', default=0.4)
@click.option('--metrics-lookback-period-minutes', default=15)
@click.option('--cool-down-period-minutes', default=5)
@click.option('--scale-in-factor', default=1.0)
@click.option('--scale-out-factor', default=1.0)
@click.option('--max-capacity-limit', help='Maximum capacity limit')
@click.option('--resize-policy', default=ResizePolicy.CPU_BASED.name)
def add(cluster_id, cluster_name, cluster_group, cpu_usage_upper_bound, cpu_usage_lower_bound,
        metrics_lookback_period_minutes, cool_down_period_minutes, max_capacity_limit,
        scale_in_factor, scale_out_factor, resize_policy):
    """Add an EMR cluster to be managed by this tool."""
    session = Session()
    cluster = Cluster(id=cluster_id, cluster_name=cluster_name,
                      cluster_group=cluster_group, cpu_usage_upper_bound=cpu_usage_upper_bound,
                      cpu_usage_lower_bound=cpu_usage_lower_bound,
                      metrics_lookback_period_minutes=metrics_lookback_period_minutes,
                      cool_down_period_minutes=cool_down_period_minutes, max_capacity_limit=max_capacity_limit,
                      scale_in_factor=scale_in_factor, scale_out_factor=scale_out_factor, resize_policy=resize_policy)
    cluster.initial_managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)[
        'ManagedScalingPolicy']
    cluster.current_managed_scaling_policy = cluster.initial_managed_scaling_policy
    if max_capacity_limit is None:
        cluster.max_capacity_limit = cluster.initial_max_units
    session.add(cluster)
    session.commit()
    session.close()


@click.command()
@click.option('--cluster-id', required=True, help='EMR cluster ID')
@click.option('--cpu-usage-upper-bound')
@click.option('--cpu-usage-lower-bound')
@click.option('--metrics-lookback-period-minutes')
@click.option('--cool-down-period-minutes')
@click.option('--max-capacity-limit')
@click.option('--resize-policy')
@click.option('--scale-in-factor')
@click.option('--scale-out-factor')
def modify(cluster_id, cpu_usage_upper_bound, cpu_usage_lower_bound,
           metrics_lookback_period_minutes, cool_down_period_minutes,
           max_capacity_limit, resize_policy, scale_in_factor, scale_out_factor):
    """Modify a cluster configuration"""
    session = Session()
    cluster: Cluster = session.get(Cluster, cluster_id)
    if cpu_usage_upper_bound is not None:
        cluster.cpu_usage_upper_bound = cpu_usage_upper_bound
    if cpu_usage_lower_bound is not None:
        cluster.cpu_usage_lower_bound = cpu_usage_lower_bound
    if metrics_lookback_period_minutes is not None:
        cluster.metrics_lookback_period_minutes = metrics_lookback_period_minutes
    if cool_down_period_minutes is not None:
        cluster.cool_down_period_minutes = cool_down_period_minutes
    if max_capacity_limit is not None:
        cluster.max_capacity_limit = max_capacity_limit
    if resize_policy is not None:
        cluster.resize_policy = resize_policy
    if scale_in_factor is not None:
        cluster.scale_in_factor = scale_in_factor
    if scale_out_factor is not None:
        cluster.scale_out_factor = scale_out_factor
    session.commit()
    session.close()


@click.command()
def list_cluster():
    """List info of all EMR clusters."""
    session = Session()
    clusters = session.query(Cluster).all()
    dicts = []
    for cluster in clusters:
        dicts.append({'Cluster ID': cluster.id,
                      'Cluster Name': cluster.cluster_name,
                      'Resize Policy': cluster.resize_policy.name,
                      'CPU Upper Bound': cluster.cpu_usage_upper_bound,
                      'CPU Lower Bound': cluster.cpu_usage_lower_bound,
                      'Max Unit Limit': cluster.max_capacity_limit,
                      'Lookback Period': cluster.metrics_lookback_period_minutes,
                      'Cool Down': cluster.cool_down_period_minutes})
    table = tabulate(dicts, headers="keys", tablefmt="grid")
    click.echo(table)
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
@click.option('--num-executors', default='1')
@click.option('--executor-cores', default='1')
@click.option('--executor-memory', default='1G')
def run_test_job(cluster_id, job_number, num_executors, executor_memory, executor_cores):
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
                '--executor-memory', executor_memory,
                '--num-executors', str(num_executors),
                '--executor-cores', str(executor_cores),
                "--conf", "spark.dynamicAllocation.enabled=false",
                '--class', 'org.apache.spark.examples.SparkPi',
                '/usr/lib/spark/examples/jars/spark-examples.jar',
                '1000000'
            ]
        }
    }
    for _ in range(job_number):
        emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )


@click.command()
@click.option('--dry-run', is_flag=True, help='Dry run mode')
def test(dry_run):
    ctx = click.get_current_context()
    # for cluster in clusters:
    #     ctx.invoke(run_test_job, cluster_id=cluster.id, job_number=10)

    def run_or_kill():
        session = Session()
        clusters = session.query(Cluster).filter(Cluster.active == True).all()
        for cluster in clusters:
            if random.choice([True, False]):
                job_number = random.randint(1, 5)
                ctx.invoke(run_test_job, cluster_id=cluster.id, job_number=job_number, num_executors='10')
            else:
                job_number = random.randint(1, 5)
                ctx.invoke(kill_test_job, cluster_id=cluster.id, job_number=job_number)
        session.close()

    scheduler = BackgroundScheduler()
    scheduler.add_job(run_or_kill, 'interval', seconds=60*5)
    scheduler.add_job(run, 'interval', args=[dry_run, None], seconds=30)
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        click.echo("Scheduler shutdown successfully.")


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


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
@click.option('--all-clusters', '-a', is_flag=True, help='Reset all clusters.')
def reset(cluster_id, all_clusters):
    """Reset clusters."""
    session = Session()
    clusters = session.query(Cluster).all()
    for cluster in clusters:
        if cluster.initial_max_units and (cluster.id == cluster_id or all_clusters):
            cluster.modify_scaling_policy(max_units=cluster.initial_max_units)
            click.echo(f'Reset cluster {cluster.id} to initial max capacity {cluster.initial_max_units}')
            emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                                  ManagedScalingPolicy=cluster.current_managed_scaling_policy)
    session.commit()
    session.close()


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
@click.option('--all-clusters', '-a', is_flag=True, help='Disable all clusters.')
def disable_cluster(cluster_id, all_clusters):
    """Disable an EMR cluster."""
    clusters = []
    session = Session()
    if cluster_id:
        clusters.append(session.get(Cluster, cluster_id))
    elif all_clusters:
        clusters = session.query(Cluster).all()
    for cluster in clusters:
        cluster.active = False
    session.commit()
    session.close()


@click.command()
@click.option('--cluster-id', help='EMR cluster ID')
@click.option('--all-clusters', '-a', is_flag=True, help='Enable all clusters.')
def enable_cluster(cluster_id, all_clusters):
    """Enable an EMR cluster."""
    clusters = []
    session = Session()
    if cluster_id:
        clusters.append(session.get(Cluster, cluster_id))
    elif all_clusters:
        clusters = session.query(Cluster).all()
    for cluster in clusters:
        cluster.active = True
    session.commit()
    session.close()


cli.add_command(add, 'add-cluster')
cli.add_command(modify, 'modify-cluster')
cli.add_command(list_cluster, 'list-clusters')
cli.add_command(delete_cluster, 'delete-cluster')
cli.add_command(describe_cluster, 'describe-cluster')
cli.add_command(run_test_job, 'run-test-job')
cli.add_command(kill_test_job, 'kill-test-job')
cli.add_command(start, 'start')
cli.add_command(reset, 'reset')
cli.add_command(disable_cluster, 'disable-cluster')
cli.add_command(enable_cluster, 'enable-cluster')
cli.add_command(test, 'test')

if __name__ == '__main__':
    cli()
