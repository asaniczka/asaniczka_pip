from setuptools import setup

setup(
    name='asaniczka',
    version='1.1.2',
    author='Asaniczka',
    author_email='asaniczka@gmail.com',
    description='All my commonly custom defined fuctions',
    long_description='This package is a neat wrapper of all my commonly used functions like requests with advanced error handling, setting up loggers and such. The idea is to have it on pypi so I can use these functions in productions',
    long_description_content_type='text/markdown',
    url='https://github.com/asaniczka/asaniczka_pip',
    packages=['asaniczka'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.10'
    ],
    install_requires=[
        'pytz>=2022.1',
        'requests>=2.31'
    ],
)
