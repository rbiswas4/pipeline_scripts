import glob
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from funcs import summarize

paramfiles = glob.glob('*params.hdf')
# fnames = list(fname.split('_params')[0] + '.hdf' for fname in paramfiles)

slist = Parallel(n_jobs=-1)(delayed(summarize)(fname) for fname in paramfiles)
pdf = pd.concat(slist)
pdfs = pd.concat(list(pd.read_hdf(paramfile) for paramfile in paramfiles))
pdfs = pdfs.join(pdf)
pdfs.to_hdf('ddf_params.hdf', '0')
