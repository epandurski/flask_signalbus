"""
Flask-SignalBus
---------------

TODO
"""
import sys
from setuptools import setup

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


setup(
    name='Flask-SignalBus',
    version='0.1',
    url='https://github.com/epandurski/flask_signalbus',
    license='BSD',
    author='Evgeni Pandurski',
    author_email='epandurski@github.com',
    description='A Flask-SQLAlchemy extension for atomically sending messages (signals) over a message bus',
    long_description=__doc__,
    packages=['flask_signalbus'],
    zip_safe=True,
    platforms='any',
    setup_requires=pytest_runner,
    install_requires=[
        'Flask-SQLAlchemy>=1.0',
    ],
    tests_require=[
        'pytest',
        'mock',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
