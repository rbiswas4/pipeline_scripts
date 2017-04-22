import glob
import pandas as pd
import numpy as np
from funcs import summarize

paramfiles = glob.glob('sim*params.hdf')
fnames = list(fname.split('_params')[0] + '.hdf' for fname in paramfiles)

first = True
for i, fname in enumerate(fnames):
    print(fname, paramfiles[i])
    df = pd.read_hdf(fname)
    if first:
        df.to_hdf('ddf_lcs.hdf', '0')
    else:
        df.to_hdf('ddf_lcs.hdf', '0', 'a')
