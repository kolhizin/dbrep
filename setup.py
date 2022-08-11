from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Package to sync tables across DBs, i.e. EL in ELT/ETL'
LONG_DESCRIPTION = 'Package provides connection-management and tools to make incremental data updates between different DBs'
# Setting up
setup(
        name="dbrep", 
        version=VERSION,
        author="Valentin Stepanovich",
        author_email="<kolhizin@gmail.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], 
        keywords=['python', 'elt', 'etl', 'el', 'db', 'replicate', 'sync'],
        classifiers= [
            "Development Status :: 1 - Planning",
            "Intended Audience :: Education",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Programming Language :: Python :: 3",
            "Operating System :: OS Independent",
            "Topic :: Database"
        ]
)