from setuptools import setup, find_packages

setup(
    name="spotify_ETL",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    long_description=open("README.md").read(),
    install_requires=["pandas", "python-dotenv", "apache-airflow", "spotipy"],
)
