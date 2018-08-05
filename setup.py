#!/usr/bin/env python
"""
Parsl based cwl runner
"""
from setuptools import setup

setup(
    name='cwl_parsl',
    version='0.0.1',
    description='Parsl based cwl runner',
    url='https://github.com/EiffL/cwl-parsl',
    maintainer='Francois Lanusse',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    packages=['cwl_parsl'],
    entry_points={
        'console_scripts':['cwl_parsl=cwl_parsl.main:main']
    },
    install_requires=['parsl>0.6.0','cwltool==1.0.20180721142728']
)
