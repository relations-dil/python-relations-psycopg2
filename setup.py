#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setup(
    name="relations-psycopg2",
    version="0.6.6",
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_psycopg2'
    ],
    install_requires=[
        'psycopg2==2.8.6',
        'relations-postgresql>=0.6.1'
    ],
    url="https://github.com/relations-dil/python-relations-postgresql",
    author="Gaffer Fitch",
    author_email="relations@gaf3.com",
    description="DB Modeling for PostgreSQL using the psycopg2 library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license_files=('LICENSE.txt',),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ]
)
