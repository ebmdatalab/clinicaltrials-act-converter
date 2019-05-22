import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="clinicaltrials-act-converter",
    version="0.0.1",
    author="Seb Bacon",
    author_email="seb.bacon@gmail.com",
    description="A package that uses Google Cloud Services to generate a CSV from CT.gov zip",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ebmdatalab/clinicaltrials-act-converter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "requests",
        "google-api-python-client",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "xmltodict",
        "lxml",
        "bs4",
        "python-dateutil",
    ],
)
