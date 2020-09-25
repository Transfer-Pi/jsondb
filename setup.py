import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jsondb-codekage", # Replace with your own username
    version="0.0.1",
    author="Viraj Patel",
    author_email="vptl185@gmail.com",
    description="JSONDB.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Transfer-PI/watcher",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)