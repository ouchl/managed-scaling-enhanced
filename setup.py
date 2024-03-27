# setup.py
from setuptools import setup, find_packages

setup(
    name='managed-scaling-enhanced',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'boto3==1.34.14',
        'loguru==0.7.2',
        'requests>=2.25.1',
        'sqlalchemy==2.0.25',
        'apscheduler==3.10.4',
        'click==8.1.7',
        'orjson==3.9.13'
    ],
    entry_points={
        'console_scripts': [
            'mse=managed_scaling_enhanced.cli:cli',
        ],
    },
)
