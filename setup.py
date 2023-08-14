import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ez_zk_client",
    version="0.0.2",
    author="Sheauren Wang",
    author_email="sheauren@gmail.com",
    description="Using Kazoo to implement specific features of ZooKeeper, such as: program survival monitoring (alive), global configuration, single-point resource lock, and multi-selection resource lock.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=['kazoo'],
    url="https://github.com/sheauren/ez_zk_client",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: MIT License"        
    ],
    python_requires='>=3.5',
)