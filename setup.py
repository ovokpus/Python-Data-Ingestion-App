from setuptools import setup, find_packages

setup(
    name="ingestion",
    version="0.0.1",
    author="Ovo Technologies",
    author_email="ovo@live.ca",
    description="Ingestion service for Ovo's portfolio",
    keywords="learn python advanced project",
    url="http://www.enchristosventures.com",
    packages=find_packages(),
    entry_points={"console_scripts": 
        ["ingestiond = ingest.backend:main"]},
    install_requires=[
        "spacy==3.4.3",
        "spacy-lookups-data==1.0.3",
    ],
    extras_require={
        "dev": [
            "pytest==6.1.2",
        ]
    }
)