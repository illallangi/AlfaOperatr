import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="alfaoperatr",
    version="0.0.1",
    author="Andrew Cole",
    author_email="andrew.cole@illallangi.com",
    description="Creates Kubernetes resources by processing custom resources through a Jinja2 template.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/illallangi/AlfaOperatr",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points = {
        'console_scripts': ['alfaoperatr=alfaoperatr:__main__.main'],
    },
    install_requires=[
        "Click",
        "aiohttp",
        "jinja2",
        "jinja2_ansible_filters",
        "jmespath",
        "more_itertools",
        "netaddr",
        "pyyaml",
        "requests",
        "six"
    ]
)