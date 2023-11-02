from codecs import open
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="bivalve",
    version="1.01",
    description="A bi-directional shell-like socket protocol framework using asyncio.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lainproliant/bivalve",
    author="Lain Musgrove (lainproliant)",
    author_email="lainproliant@gmail.com",
    license="BSD",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="network sockets protocol shell",
    packages=find_packages(),
    install_requires=[],
    extras_require={},
    package_data={"bivalve": ["LICENSE"]},
    data_files=[],
    entry_points={"console_scripts": ["bivalve = bivalve.cli:main"]},
)
