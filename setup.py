from setuptools import setup, find_packages

setup(
    name             = 'congregation',
    version          = '0.0.0.1',
    packages         = find_packages(),
    license          = 'MIT',
    url              = 'https://github.com/multiparty/congregation',
    description      = 'Query compiler for relational MPC workflows based on the conclave system.',
    long_description = open('README.md').read(),
    test_suite       = 'pytest',
    tests_require    = ['pytest'],
)