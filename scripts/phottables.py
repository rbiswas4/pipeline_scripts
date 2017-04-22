__all__ = ('summarizePhotTables')

import numpy as np
import pandas as pd

def summarizePhotTables(paramdf, lcdf, vals, aggfuncs, SNRmin=1.0):
    """
    Summarize the light curves in the photometry table lcdf  
    """
    lcdf = lcdf.copy()
    if 'SNR' not in lcdf.columns:
        lcdf['SNR'] = lcdf['flux'] / lcdf['fluxerr']
    lcdf = lcdf.query('SNR > @SNRmin')
    grouped = lcdf.groupby(['snid', 'band'])
    mapdict = dict(tuple(zip(vals, aggfuncs)))
    summary = grouped.agg(mapdict).unstack()
    summary.columns = list('_'.join(col).strip() for col in summary.columns.values)
    namedicts = dict((col, 'NOBS_' + col.split('count_')[-1]) for col in summary.columns if 'count' in col)
    summary.rename(columns=namedicts, inplace=True)
    paramdf = paramdf.join(summary)
    return paramdf, lcdf, summary
