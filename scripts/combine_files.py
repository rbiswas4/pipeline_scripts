from __future__ import absolute_import
import glob
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
# from funcs import summarize
from phottables import summarizePhotTables
import os


def get_dataframes(pfname, lcfname):
    pdf = pd.read_hdf(pfname)
    lcdf = pd.read_hdf(lcfname)
    return pdf, lcdf

def get_fnames():
    # Rewrite for the output
    paramfiles = glob.glob('*params.hdf')
    lcfiles = list(fname.split('_params')[0] + '.hdf' for fname in paramfiles)
    return zip(paramfiles, lcfiles)

def summarize(files, SNRmin=-100000.0):
    paramfile, lcfile = files
    pdf, lcdf = get_dataframes(paramfile, lcfile)
    lcdf['SNR'] = lcdf['flux']/lcdf['fluxerr']
    #tileID = np.int(lcfile.split('_')[-1].split('.hdf')[0])
    #lcdf['tileID'] = tileID
    lcdf['snid'].astype('int')

    vals = ('SNR', 'mjd', 'zp', 'fieldID')
    aggfuncs = (max, [max, min], 'count', 'first', 'count')
    paramdf, lcdf, summary = summarizePhotTables(pdf, lcdf, vals, aggfuncs, SNRmin=SNRmin)
    return paramdf



# fnames = list(fname.split('_params')[0] + '.hdf' for fname in paramfiles)
files = get_fnames()
slist = Parallel(n_jobs=-1)(delayed(summarize)(fname) for fname in files)
pdf = pd.concat(slist)
#pdfs = pd.concat(list(pd.read_hdf(paramfile) for paramfile in paramfiles))
#pdfs = pdfs.join(pdf)
pdf.index = pdf.index.astype(np.int)
pdf.to_hdf('ddf_params.hdf', '0')
first = True
numSN = 0
numSNObs = 0
for (paramfile, fname) in files:
    print(fname, paramfile)
    df = pd.read_hdf(fname)
    pdf = pd.read_hdf(paramfile)
    df['SNR'] = df['flux']/df['fluxerr']
    print(fname)
    tileID = np.int(fname.split('_')[-1].split('.hdf')[0])
    df['tileID'] = tileID
    df.tileID = df.tileID.astype(np.int)
    df.snid = df['snid'].astype('int')
    df.fieldID = df.fieldID.astype(np.int)
    nObs = len(df)
    nSN = df.snid.unique().size
    numSNObs += nSN
    nSNParams = len(pdf) 
    numSN += nSNParams
    print(tileID, nObs, nSN, numSNObs, nSNParams, numSN)
    if first:
        print('creating file')
        df.to_hdf('ddf_lcs.hdf', key=str(tileID), format='table')
        first = False
        print('setting first off', first)
        store = pd.HDFStore('ddf_lcs.hdf')
    else:
        if len(df) > 0:
            try:
                store.append(key=str(tileID), value=df)
                print('appending to file')
            except:
                print('appending failed for {0}'.format(len(df)))
        else:
            print('skipping as len(df) = 0')
