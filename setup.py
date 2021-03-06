import os
import os.path

from setuptools import setup, find_packages, Extension
import versioneer

# Default description in markdown
LONG_DESCRIPTION = open('README.md').read()


PKG_NAME     = 'action-tool'
AUTHOR       = 'Rafael Gonzalez'
AUTHOR_EMAIL = 'astrorafael@gmail.com'
DESCRIPTION  = 'Deployment tool for Airflow ACTION project pilots',
LICENSE      = 'MIT'
KEYWORDS     = 'Astronomy Python CitizenScience LightPollution'
URL          = 'https://github.com/actionprojecteu/action-tool/'
DEPENDENCIES = [
    "matplotlib",
    "scikit-learn",
    'tabulate',
    'Pillow'
]

CLASSIFIERS  = [
    'Environment :: Console',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 3.7',
    'Topic :: Scientific/Engineering :: Astronomy',
    'Topic :: Scientific/Engineering :: Atmospheric Science',
    'Development Status :: 4 - Beta',
]

PACKAGE_DATA = {
    'actiontool.streetspectra': [
        'data/dags/*.py',
        'data/sql/*.sql'
    ],
}

SCRIPTS = [
    "scripts/actiontool",
    "scripts/streetool",
]


DATA_FILES  = [ 

]

setup(
    name             = PKG_NAME,
    version          = versioneer.get_version(),
    cmdclass         = versioneer.get_cmdclass(),
    author           = AUTHOR,
    author_email     = AUTHOR_EMAIL,
    description      = DESCRIPTION,
    long_description_content_type = "text/markdown",
    long_description = LONG_DESCRIPTION,
    license          = LICENSE,
    keywords         = KEYWORDS,
    url              = URL,
    classifiers      = CLASSIFIERS,
    packages         = find_packages("src"),
    package_dir      = {"": "src"},
    install_requires = DEPENDENCIES,
    scripts          = SCRIPTS,
    package_data     = PACKAGE_DATA,
    data_files       = DATA_FILES,
)
