# cwl-parsl
A demonstrator for a Parsl based CWL runner.


To install:
```
$ pip install --user git+https://github.com/EiffL/cwl-parsl
```

Checkout the documentation of [cwltool](https://github.com/common-workflow-language/cwltool)
to learn more about the command line options

Run an example locally, using a default configuration of 8 parsl threads:
```
$ cwlparsl scatter-wf2.cwl scatter_job.yml
```

## Staging and caching directories

cwlparsl provides the `outdir`, `cachedir`, and `basedir` options
which define respectively:
  - the output directory of the final workflow products
  - a caching directory where each step will be cached to recover from a failure
  - the base working directory (to stage input/outputs for each pipeline step)
```bash
$ cwlparsl --parsl cori --shifter \
  --outdir=/global/cscratch1/sd/flanusse/metacal-pipeline \
  --cachedir=/global/cscratch1/sd/flanusse/workdir/cache/ \
  --basedir=/global/cscratch1/sd/flanusse/workdir/ \
  tools/metacal-wf.cwl config/metacal-wf-testing.yml
```



## Run configurations on cori

This is assuming you are logged on cori, to run using shifter, and on the debug queue:
```
$ cwlparsl --parsl cori-debug --shifter scatter-wf2.cwl scatter_job.yml
```
 Same thing but on on the regular queue:
```
$ cwlparsl --parsl cori --shifter scatter-wf2.cwl scatter_job.yml
```
And to run through an interactive session:
```
$ salloc -N 1 -q interactive -C haswell -t03:00:00 -L SCRATCH
$ cwlparsl --shifter scatter-wf2.cwl scatter_job.yml
```
