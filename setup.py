from setuptools import find_packages, setup
import os

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    requirements = f.read()

setup(
    name='prozorro_crawler',
    version_format='{tag}',
    setup_requires=['setuptools-git-version'],
    description='',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=requirements,
)
