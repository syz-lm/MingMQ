from setuptools import find_packages, setup

setup(
    name='mingmq',
    version='2.0.1',
    url='https://github.com/zswj123/mingmq',
    license='',
    maintainer='zswj123',
    maintainer_email='l2se@sina.cn',
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
