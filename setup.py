from setuptools import setup

setup(
    name='logless-logging',
    packages=['logless_logging'],
    version='0.0.3',
    description='LogLess logging module',
    author='Darko Ronic',
    author_email='darko.ronic@gmail.com',
    license='MIT',
    url='http://github.com/apolloFER/logless-python',
    install_requires=[
        'kinaggregator',
        'msgpack-python',
        'boto3',
        'six'
    ]
)
