# adc-pycsw
Setup and deployment of pycsw for the Arctic Data Centre (ADC) project

## Installation 

- clone pycsw

```git clone https://github.com/geopython/pycsw```

- clone adc-pycsw (this repository)

```git clone https://github.com/epifanio/adc-pycsw```

- copy the plugin files into the source code

```cp adc-pycsw/blob/main/plugins/repository/solr_metno.py pycsw/pycsw/plugins/repository/```
```cp adc-pycsw/blob/main/plugins/repository/solr_helper.py pycsw/pycsw/plugins/repository/```

- create a python environment (using conda for convenience)

```conda create -n pycswdev```

- Activate newly created environment

```conda activate pycswdev```

- install pycsw and its dependencies


```
cd pycsw
pip install -r requirements-dev.txt
pip install -e .
```

## Setup


- make a copy of the pycsw configuration exasmple file and name it `default.cfg` edit its content to have the following in the `server` and `repository` section:

```
[server]
home=/home/epinux/dev/WORK/pycsw
url=http://localhost:8000/pycsw/csw.py
```

```
[repository]
database=None
source=pycsw.plugins.repository.solr_metno.SOLRMETNORepository
filter=https://solr.epinux.com/solr/mmd
```

- from the pycdwdev environment export the ```MMD_TO_ISO``` environment variable, to the path for the xslt used to convert MMD records to ISO, e.g.:

```export MMD_TO_ISO="mmd/xslt/mmd-to-inspire.xsl"```


- activate the pycswdev environment and run the pycsw wsgi app
```
conda activate pycswdev
python ./pycsw/wsgi.py
```

pray!