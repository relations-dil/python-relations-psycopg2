#!/usr/bin/env python

from setuptools import setup, find_packages
setup(
    name="relations-psycopg2",
    version="0.4.5",
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_psycopg2'
    ],
    install_requires=[
        'psycopg2==2.8.6'
    ]
)
