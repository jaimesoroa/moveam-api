from setuptools import find_packages
from setuptools import setup

with open("requirements.txt") as f:
    content = f.readlines()
requirements = [x.strip() for x in content if "git+" not in x]

setup(name='moveam-api',
      version="0.0.1",
      description="Moveam API",
      license="",
      author="Jaime Soroa",
      author_email="jsoroat@moveam.com",
      url="https://github.com/jaimesoroa/moveam-api",
      install_requires=requirements,
      packages=find_packages(),
      test_suite="tests",
      # include_package_data: to install data from MANIFEST.in
      include_package_data=True,
      zip_safe=False)
