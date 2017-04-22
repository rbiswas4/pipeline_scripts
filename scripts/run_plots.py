#!/usr/bin/env python

# run using:
# nohup ./run_plots.py > run.log 2>&1 &
import os
from collections import OrderedDict
import numpy as np
import pandas as pd
from copy import copy
from joblib import Parallel, delayed
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')
import sys

import sncosmo
from analyzeSN import LightCurve, ResChar
from lsst.sims.catUtils.supernovae import SNObject

minion_params = pd.read_hdf('ddf_params.hdf')
minion_params.sort_values(by='t0', inplace=True)
minion = pd.read_hdf('ddf_lcs.hdf', '0')

def paramDict(snid, paramdf):
    cols = ['t0', 'x0', 'x1', 'c','z'] 
    odict = OrderedDict(paramdf.ix[snid, cols])
    return odict, paramdf.ix[snid, 'ra'], paramdf.ix[snid, 'dec']

def inferParams(snid,
                snmodel,
                paramsDF,
                lcsDF,
                infer_method=sncosmo.fit_lc,
                minsnr=0.):
    """
    infer the parameters for the ith supernova in the simulation
    """
    pdict, ra, dec = paramDict(snid, paramsDF)
    snmodel.setCoords(ra, dec)
    snmodel.mwEBVfromMaps()
    snmodel.set(**pdict)
    truth = copy(snmodel.equivalentSNCosmoModel())#.copy()
    #print(model)
    #z = params.ix[snid, 'z']
    lcinstance = LightCurve(lcsDF.query('snid==@snid'))
    fig = None
    try:
        print('trying fit')
        resfit = infer_method(lcinstance.snCosmoLC(), snmodel.equivalentSNCosmoModel(),
                              vparam_names=['t0', 'x0', 'x1', 'c'], 
                              modelcov=False, minsnr=minsnr)#, bounds=dict(z=(0.0001, 1.6)))
        reschar = ResChar.fromSNCosmoRes(resfit)
        print('fit passed')
    except:
        reschar = 'failure'
        print('failed for SNID {0} with {1} points'.format(snid, len(lcinstance.snCosmoLC())))
    #if reschar != 'failure':
    #    pass
    #    fig = None#sncosmo.plot_lc(lcinstance.snCosmoLC(), model=(truth, reschar.sncosmoModel))
    return snid, lcinstance, reschar, truth, fig

snidvals = minion_params.index.values
print('The list of SN has {} SN'.format(len(snidvals)))

snmodel = SNObject(ra=30., dec=-30.)
def writevals(snid, fh):
    x = inferParams(snid,
                    snmodel,
                    paramsDF=minion_params,
                    lcsDF=minion,
                    infer_method=sncosmo.fit_lc,
                    minsnr=0.)
    if x[2] != 'failure':
        covs = map(str, x[2].covariance.values[np.triu_indices(4)])
        params = map(str, x[2].parameters.values)
        std = str(x[2].mu_variance_linear()**0.5)
        ss = ','.join(params + covs + [std])
        # print('cov = ' + ' '.join(covs))
        # print('params = ' + ' '.join(params))
        # print('std = ' + str(std))
    else:
        ss = ','.join(map(str, np.repeat(np.nan, 20)))
    s = '{0:d},{1}\n'.format(snid, ss)
    sys.stdout.flush()
    print ('writing results for SN {}'.format(snid))
    fh.write(s)
    
def runGroup(i): 
    fname = 'files/test_{}.csv'.format(i)
    arr = splitarrs[i]
    print('starting job {0} with {1} SN'.format(i, len(arr)))
    if os.path.exists(fname):
        with open(fname, 'a') as fh:
            _ = list(writevals(snid, fh)for snid in arr)
    else:
        with open(fname, 'w') as fh:
            _ = list(writevals(snid, fh)for snid in arr)
# with open('test.csv', 'w') as fh:
#    writevals(snidvals[0])
numvals = 200
splitarrs = np.array_split(snidvals, numvals)
#splitarrs = []
#splitarrs.append(snidvals[:11])
#print (splitarrs[0], snidvals[:11])
#runGroup(0)
#x = inferParams(164854005, snmodel, paramsDF=sandwich_params,
#                lcsDF=sandwich, infer_method=sncosmo.fit_lc,
#                minsnr=0.)
#print(x[0], x[1], x[2], x[3], x[4])
# with open('testing.csv', 'w') as fh:
#    writevals(165504067, fh)
Parallel(n_jobs=-1)(delayed(runGroup)(numval) for numval in range(numvals))
