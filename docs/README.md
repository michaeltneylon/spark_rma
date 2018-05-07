# Documentation 

Build and serve documentation with Sphinx.

Install dependencies: `pip install -r requirements.txt`

Install packages dependencies `pip install -r ../helper/requirements.txt`, or alternatively install fastparquet 
with conda: `conda install fastparquet`.

Install package dependencies: `pip install -r ../spark_rma/requirements.txt`

Install the spark_rma package: 

```
cd ..
python setup.py install
```

Then build the docs:

```
cd docs 
make html
```

Serve the docs 
```
cd _build/html
python -m http.server # python 3
```

