from setuptools import setup, find_packages

setup(
    name='spark_rma',
    packages=find_packages(),
    version='0.2.2',
    description="Robust Multi-Array Average in Spark",
    author='Michael T. Neylon',
    author_email='neylon_michael_t@lilly.com',
    url='https://github.com/michaeltneylon/spark_rma',
    download_url='https://github.com/michaeltneylon/spark_rma/archive/0.2.2'
                 '.tar.gz',
    license='Apache License 2.0',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'
    ]
)
