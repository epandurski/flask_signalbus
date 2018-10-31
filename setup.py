"""
Flask-SignalBus
-------------

TODO
"""
from setuptools import setup


setup(
    name='Flask-SignalBus',
    version='0.98',
    url='https://github.com/epandurski/flask_signalbus',
    license='BSD',
    author='Evgeni Pandurski',
    author_email='epandurski@github.com',
    description='A flask extension for atomically sending messages (signals) over a message bus',
    long_description=__doc__,
    # if you would be using a module instead use py_modules instead of packages:
    # py_modules=['flask_signalbus'],
    packages=['flask_signalbus'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    python_requires='>=3.5',
    install_requires=[
        'Flask',
        'Flask-SQLAlchemy',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
