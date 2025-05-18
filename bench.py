from __future__ import annotations

from timeit import timeit

import pandas as pd
from pandas.compat.pickle_compat import load as read_pickle
from fin_models.enums import Freq
from fin_models.services import store


LOOPS = 1000


if __name__ == "__main__":
    amd = store.get("AMD", freq=Freq.min_1)


    # to_pickle_4 = timeit(lambda: amd.to_pickle('/tmp/amd_min_1.pickle', protocol=4), number=LOOPS)
    # print(f'{to_pickle_4=}s')
    #
    # to_pickle_5 = timeit(lambda: amd.to_pickle('/tmp/amd_min_1.pickle', protocol=5), number=LOOPS)
    # print(f'{to_pickle_5=}s')

    amd.to_pickle("/tmp/amd_min_1.pickle")
    to_pickle_4 = timeit(lambda: pd.read_pickle("/tmp/amd_min_1.pickle"), number=LOOPS)
    print(f"{to_pickle_4=}s")

    amd.to_pickle("/tmp/amd_min_1.pickle", protocol=5)
    to_pickle_5 = timeit(lambda: amd.to_pickle("/tmp/amd_min_1.pickle"), number=LOOPS)
    print(f"{to_pickle_5=}s")
