import os
from setuptools import setup, find_packages

# Get the absolute path to the library
lib_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'src', 'pythonwrapper', 'libmdlclient.so'))

setup(
    name="mdlin",
    version="0.1.0",
    packages=find_packages(),
    package_data={
        'mdlin': [lib_path],
    },
    include_package_data=True,
    install_requires=[],
    author="Austin Li",
    description="MDLin Python Client Wrapper",
    python_requires=">=3.6",
)

