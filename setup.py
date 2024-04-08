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
        'apscheduler==3.10.4',
        'click==8.1.7'
    ],
    entry_points={
        'console_scripts': [
            'mse=managed_scaling_enhanced.cli:cli',
        ],
    },
)
