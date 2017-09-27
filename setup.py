"""
Configuration de la création du package dvf_plus

  python setup.py sdist

Le résultat est dans le répertoire dist

Installation de l'archive sur un autre environnement

  pip install dvf-plus-0.2.1.zip


"""

from setuptools import setup
from setuptools import find_packages


setup(
    name='dvf_plus',
    version='0.2.2',
    description='module permettant la création d\'une base DVF+',
    author='Antoine HERMAN',
    author_email='antoine.herman@cerema.fr',
    url='https://github.com/dvf-cerema/dvf_plus',
    package_dir={'dvf_plus': 'dvf_plus'},
    packages = find_packages(),
    include_package_data=True,
    install_requires=['pg>=0.1.5'],
    entry_points = {
        'console_scripts': ['dvfplus=dvf_plus.__main__:main'],
    }
)

# eof