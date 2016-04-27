from contextlib import closing

import os
from setuptools import setup

with closing(open(os.path.join(os.path.dirname(__file__), 'README.md'))) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='lograph',
    version='0.0.1',
    packages=['lograph'],
    include_package_data=True,
    license='MIT Licenses',
    description='Simple code snippet to generate graph from log.',
    long_description=README,
    url='https://github.com/initialjk/lograph',
    author='Lee jaekang',
    author_email='prog.jk@gmail.com',
    classifiers=[
        'Environment :: Pure Python',
        'Framework :: matplotlib',
        'Intended Audience :: Graph, Chart',
        'License :: OSI Approved :: MIT Licenses',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
        'matplotlib~=1.5.1',
    ],
    tests_require=[
    ],
)
