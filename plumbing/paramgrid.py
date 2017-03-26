__all__ = ['cartesian']

import numpy as np

def cartesian(larray):
    """
    returns a two dimensional array with each element of the array being
    an element of the cartesian product of the sets

    Parameters
    ----------
    larray : sequence of numpy arrays of parameter values that should be
        gridded up

    Returns
    -------
    an array with elements that are cartesian products of the arrays
    """
    return np.array(np.meshgrid(*larray)).T.reshape(-1, len(larray))
