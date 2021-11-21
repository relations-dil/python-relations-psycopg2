#!/usr/bin/env python

from setuptools import setup, find_packages
setup(
    name="python-relations-psycopg2",
    version="0.6.2",
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_psycopg2'
    ],
    install_requires=[
        'psycopg2==2.8.6'
    ]
)
