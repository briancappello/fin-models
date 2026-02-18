from __future__ import annotations

from timeit import timeit

import pandas as pd

from pandas.compat.pickle_compat import load as read_pickle

from fin_models.enums import Freq
from fin_models.services import store


LOOPS = 100


if __name__ == "__main__":
    amd = store.get("AMD", freq=Freq.min_1)

    # store=0.014428415789998325s
    current = timeit(lambda: store.get("AMD", freq=Freq.min_1), number=LOOPS)
    print(f"store={current / LOOPS}s")

    """
    to_pickle_4/LOOPS=0.007457234550001885s
    to_pickle_5/LOOPS=0.006524793400003546s
    from_pickle_4/LOOPS=0.011095399870000619s
    from_pickle_5/LOOPS=0.01088756387000103s
    """
    to_pickle_4 = timeit(
        lambda: amd.to_pickle("/tmp/amd_min_1.pickle", protocol=4), number=LOOPS
    )
    print(f"{to_pickle_4/LOOPS=}s")

    to_pickle_5 = timeit(
        lambda: amd.to_pickle("/tmp/amd_min_1.pickle", protocol=5), number=LOOPS
    )
    print(f"{to_pickle_5/LOOPS=}s")

    amd.to_pickle("/tmp/amd_min_1.pickle")
    from_pickle_4 = timeit(lambda: pd.read_pickle("/tmp/amd_min_1.pickle"), number=LOOPS)
    print(f"{from_pickle_4/LOOPS=}s")

    amd.to_pickle("/tmp/amd_min_1.pickle", protocol=5)
    from_pickle_5 = timeit(lambda: pd.read_pickle("/tmp/amd_min_1.pickle"), number=LOOPS)
    print(f"{from_pickle_5/LOOPS=}s")

    """
    pyarrow to_parquet/LOOPS=0.13725337224000214s
    pyarrow from_parquet/LOOPS=0.06297288963999563s
    fast to_parquet/LOOPS=0.10209347547999642s
    fast from_parquet/LOOPS=0.04445640969000124s
    """
    to_parquet = timeit(
        lambda: amd.to_parquet("/tmp/amd.parquet", engine="pyarrow"), number=LOOPS
    )
    print(f"pyarrow {to_parquet/LOOPS=}s")

    from_parquet = timeit(
        lambda: pd.read_parquet("/tmp/amd.parquet", engine="pyarrow"), number=LOOPS
    )
    print(f"pyarrow {from_parquet/LOOPS=}s")

    to_parquet = timeit(
        lambda: amd.to_parquet("/tmp/amd.parquet", engine="fastparquet"), number=LOOPS
    )
    print(f"fast {to_parquet/LOOPS=}s")

    from_parquet = timeit(
        lambda: pd.read_parquet("/tmp/amd.parquet", engine="fastparquet"), number=LOOPS
    )
    print(f"fast {from_parquet/LOOPS=}s")

    """
    to_feather/LOOPS=0.06362030565000168s
    from_feather/LOOPS=0.041216971129997546s
    """
    to_feather = timeit(lambda: amd.to_feather("/tmp/amd.feather"), number=LOOPS)
    print(f"{to_feather/LOOPS=}s")

    from_feather = timeit(lambda: pd.read_feather("/tmp/amd.feather"), number=LOOPS)
    print(f"{from_feather/LOOPS=}s")

    h5_store = pd.HDFStore("/tmp/store.h5")
    to_hdf5 = timeit(lambda: h5_store.put("amd", amd, format="table"), number=LOOPS)
    print(f"{to_hdf5/LOOPS=}s")

    from_hdf5 = timeit(lambda: h5_store.get("amd"), number=LOOPS)
    print(f"{from_hdf5/LOOPS=}s")
    h5_store.close()

    # in-memory, writes to disk upon call to .close()
    pd.HDFStore("/tmp/test.h5", driver="H5FD_CORE")

    df = amd.loc[:"2025-06-12"]
    six13 = amd.loc["2025-06-13"]

    with pd.HDFStore("/tmp/store.h5") as h5_store:
        h5_store.put("amd", df, format="table")
        append = timeit(lambda: h5_store.append("amd", six13), number=1)
        print(f"hdf5 {append}s")
