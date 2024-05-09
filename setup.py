# setup.py
from setuptools import setup, find_packages

setup(
    name='managed-scaling-enhanced',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'boto3',
        'requests',
        'sqlalchemy',
        'apscheduler',
        'click',
        'orjson',
        'tabulate',
        'dataclasses',
        'python-dateutil',
        'pymysql',
        'aiohttp'
    ],
    entry_points={
        'console_scripts': [
            'mse=managed_scaling_enhanced.cli:cli',
        ],
    },
)
