# # Data Wrangling / Analysis

# ## 1 Read Data
# ### 1.1 Create Spark session
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession\
    .builder\
    .appName('wine-quality-analysis')\
    .master("local[*]")\
    .getOrCreate()

from IPython.core.display import HTML
import os
sparkUI_url='http://spark-'+os.environ.get("CDSW_ENGINE_ID")+"."+os.environ.get("CDSW_DOMAIN")
HTML("<a href='"+sparkUI_url+"'>"+sparkUI_url+"</a>")


# ### 1.2 Read Data

# ### Load the data
# We need to load data from a file in to a Spark DataFrame.
# Each row is a wine, and each column contains attributes of that wine.
#
#     Fields:
#     fixedAcidity: numeric
#     volatileAcidity: numeric
#     citricAcid: numeric
#     residualSugar: numeric
#     chlorides: numeric
#     freeSulfurDioxide: numeric
#     totalSulfurDioxide: numeric
#     density: numeric
#     pH: numeric
#     sulphates: numeric
#     Alcohol: numeric
#     Quality: discrete

wine_data_raw = spark.sql(''' SELECT * FROM wineDS_ext''')
wine_data_raw.show(3)


# ### 1.3 Basic DataFrame operations
# Dataframes essentially allow you to express sql-like statements.
# We can filter, count, and so on.
# Documentation - (http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations)
# Spark SQL - manipulate data as if it was a table

# #### Number of lines in dataset :
spark.sql("select count(*) from wineDS_ext").show()

# #### View labels and nb lines attached
spark.sql("select distinct(Quality), count(*) from wineDS_ext GROUP BY Quality").show()

# #### Correct invalid label
wine_data = wine_data_raw.filter(wine_data_raw.quality != "1")
total_wines = wine_data.count()
good_wines = wine_data.filter(wine_data.quality == 'Excellent').count()
good_wines = wine_data.filter(wine_data.quality == 'Poor').count()

"Wines total: {}, Good : {}, Poor : {}".format(total_wines,good_wines,good_wines)


# # 2. Data visualisation ( using mathplotlib and Seaborn)
# ## Feature Visualization
#
# The data vizualization workflow for large data sets is usually:
#
# * Sample data so it fits in memory on a single machine.
# * Examine single variable distributions.
# * Examine joint distributions and correlations.
# * Look for other types of relationships.
#
# [DataFrame#sample() documentation](http://people.apache.org/~pwendell/spark-releases/spark-1.5.0-rc1-docs/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)

# ### 2.1 Data Sampling
# ### Note: toPandas() => brings data localy !!!

sample_data = wine_data.sample(False, 0.5, 83).toPandas()
sample_data.transpose().head(21)


# ### 2.2 Feature Distributions with Matplotlib
# We want to examine the distribution of our features, so start with them one at a time.
# Seaborn has a standard function called [dist()](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.distplot.html#seaborn.distplot) that allows us to easily examine the distribution of a column of a pandas dataframe or a numpy array.

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import seaborn as sb

sb.distplot(sample_data['alcohol'], kde=False)

# We can examine feature differences in the distribution of our features when we condition (split) our data.
# [BoxPlot docs](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.boxplot.html)

sb.boxplot(x="quality", y="alcohol", data=sample_data)

# ### 2.3 Joint Distributions with Seaborn
# Looking at joint distributions of data can also tell us a lot, particularly about redundant features.
# [Seaborn's PairPlot](http://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.pairplot.html#seaborn.pairplot)
# let's us look at joint distributions for many variables at once.

example_numeric_data = sample_data[["fixedacidity", "volatileacidity",
                                       "citricacid", "residualsugar", "quality"]]
sb.pairplot(example_numeric_data, hue="quality")


HTML("<a href='"+sparkUI_url+"'>"+sparkUI_url+"</a>")
