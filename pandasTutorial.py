import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

dates = pd.date_range('20130101', periods=6)
print(dates)

df = pd.DataFrame(np.random.rand(6,4), index=dates, columns=list('ABCD'))
print(df)
