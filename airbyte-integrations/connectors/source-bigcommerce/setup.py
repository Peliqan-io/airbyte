#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk @ git+https://github.com/Peliqan-io/airbyte.git@master#egg=airbyte-cdk&subdirectory=airbyte-cdk/python",
]

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "connector-acceptance-test",
]

setup(
    name="source_bigcommerce",
    description="Source implementation for Bigcommerce.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
