from setuptools import setup, find_packages

setup(
    name="bdr_solrizer",
    version='4.13',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    scripts=['bin/queue_solrize'],
    install_requires=[
        'bdrocfl',
        'bdrxml>=1.2',
        'requests>=2.11.0',
        'redis>=3.0.1',
        'rq==0.13.0',
        'diskcache>=3.0.6',
        'inflection>=0.3.1',
        'roman>=3.2',
    ],
    extras_require={
        'dev':  ['responses'],
    }
)
