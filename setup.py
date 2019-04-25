'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-24 12:20:24
@LastEditTime: 2019-04-25 15:23:15
'''

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sparksdfanalyse",
    version="0.1",
    author="Laurengcy",
    author_email="Laurengcy@users.noreply.github.com",
    description="Helper functions for basic analysis of sparks DF, written in pysparks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="to be determined",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: To be determined",
        "Operating System :: Written in Mac OSX",
    ),
)