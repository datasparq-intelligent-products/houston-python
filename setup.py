from setuptools import setup, find_packages
from os import path

# Copy README.md to project description
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="houston-client",
    packages=find_packages(),
    description="Houston Python Client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="1.1.0",
    url="https://github.com/datasparq-intelligent-products/houston-python",
    author="James Watkinson, Matt Simmons & Ivan Nedjalkov",
    license="MIT",
    author_email="james.watkinson@datasparq.ai",
    install_requires=["requests==2.22.0", "requests[security]==2.22.0"],
    extras_require={"gcp": ["google-cloud-pubsub>=1.2.0"],
                    "azure": [
                        "azure-eventgrid==1.3.0",
                        "azure-mgmt-eventgrid==2.2.0"
                    ]},
    keywords=["houston"],
)
