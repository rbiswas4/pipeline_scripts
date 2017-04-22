import pandas as pd
import numpy as np
import time
__all__ = ['summarize']
def summarize(paramfilename):
    lcfilename = paramfilename.split('_params')[0] + '.hdf'
    lcoutfile =  paramfilename.split('_params')[0] + '_lc.hdf'
    tileid = paramfilename.split('_')[1]
    pp = pd.read_hdf(lcfilename)
    pp['SNR'] = pp.flux/ pp.fluxerr
    pp['tileID'] = np.int(tileid)
    s = pp.groupby('snid').agg(dict(fieldID=lambda x: np.unique(x).size, zp='count', mjd=[min, max], SNR=max))
    maxSNR = s['SNR']['max']
    s.drop('SNR', axis=1, inplace=True)
    s.columns = s.columns.droplevel(0)
    s['numFields']=s['<lambda>']
    s.drop('<lambda>', axis=1, inplace=True)
    s['SNRmax'] = maxSNR
    s.rename(columns=dict(count='numObs', fieldID='numFields', min='minMJD', max='maxMJD'), inplace=True)
    pp.to_hdf(lcoutfile, key='0') 
    return s
