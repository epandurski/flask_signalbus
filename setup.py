"""
Flask-SignalBus
---------------

Adds to Flask-SQLAlchemy the capability to *atomically* send messages
(signals) over a message bus.

The processing of each message involves three steps:

  1. The message is recorded in the SQL database as a row in a table.

  2. The message is sent over the message bus (RabbitMQ for example).

  3. Message's corresponding table row is deleted.

Normally, the sending of the recorded messages (steps 2 and 3) is done
automatically after each transaction commit, but when needed, it can
also be triggered explicitly with a method call, or through the Flask
CLI.
"""

import sys
from setuptools import setup

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


setup(
    name='Flask-SignalBus',
    version='0.5.13',
    url='https://github.com/epandurski/flask_signalbus',
    license='MIT',
    author='Evgeni Pandurski',
    author_email='epandurski@gmail.com',
    description='A Flask-SQLAlchemy extension for atomically sending messages (signals) over a message bus',
    long_description=__doc__,
    packages=['flask_signalbus', 'flask_signalbus.rabbitmq'],
    zip_safe=True,
    platforms='any',
    setup_requires=pytest_runner,
    install_requires=[
        'Flask-SQLAlchemy>=1.0',
        'marshmallow-sqlalchemy>=0.22.0',
    ],
    tests_require=[
        'pytest~=6.2',
        'pytest-cov~=2.7',
        'mock~=2.0',
        'pika~=1.3',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
