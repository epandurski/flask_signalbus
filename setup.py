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
    version='0.98',
    url='https://github.com/epandurski/flask_signalbus',
    license='BSD',
    author='Evgeni Pandurski',
    author_email='epandurski@github.com',
    description='A flask extension for atomically sending messages (signals) over a message bus',
    long_description=__doc__,
    packages=['flask_signalbus'],
    zip_safe=True,
    platforms='any',
    python_requires='>=3.5',
    setup_requires=pytest_runner,
    install_requires=[
        'Flask>=0.10',
        'SQLAlchemy>=0.8.0',
    ],
    tests_require=[
        'pytest',
        'Flask-SQLAlchemy>=1.0',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
