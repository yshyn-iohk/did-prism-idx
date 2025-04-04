from setuptools import setup, find_packages

setup(
    name="did-prism-idx",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'psycopg2-binary>=2.9.0',
        'petl>=1.6.10',
        'fastapi>=0.68.0',
        'protobuf'
    ]
)