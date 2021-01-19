from setuptools import find_packages, setup

setup(
    name='mingmq',
    version='2.0.6',
    url='',
    license='',
    maintainer='kael',
    maintainer_email='congshi.hello@gmail.com',
    description='',
    long_description='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask',
        'flask_httpauth',
        'netifaces',
        'gevent'
    ],
    entry_points="""
    [console_scripts]
    mmserver = mingmq.command:main
    mmweb = mingmq.api:main
    """
)
