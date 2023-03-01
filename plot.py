import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.pyplot as plt
from statistics import mean
import numpy as np
plt.rcParams["figure.figsize"] = [7.00, 3.50]
plt.rcParams["figure.autolayout"] = True
columns = ["credit_score", "age"]
df = pd.read_csv("customer_information.csv", usecols=columns)
print("Age vs Credit Score", df)
xs = df.credit_score
ys = df.age
plt.ylabel('Age')
plt.xlabel('Credit Score')
plt.bar(xs, ys)
plt.show()