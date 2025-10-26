## Encoding Categorical Features

In this section, we will one-hot encode categorical/string features using Spark MLlib's `OneHotEncoder` estimator.

If you are unfamiliar with one-hot encoding, there's a description below. If you're already familiar, you can skip ahead to the **One-hot encoding in Spark MLlib** section toward the bottom of the cell.

#### Categorical features in machine learning

Many machine learning algorithms are not able to accept categorical features as inputs. As a result, data scientists and machine learning engineers need to determine how to handle them. 

An easy solution would be remove the categorical features from the feature set. While this is quick, **you are removing potentially predictive information** &mdash; so this usually isn't the best strategy.

Other options include ways to represent categorical features as numeric features. A few common options are:

1. **One-hot encoding**: create dummy/binary variables for each category
2. **Target/label encoding**: replace each category value with a value that represents the target variable (e.g. replace a specific category value with the mean of the target variable for rows with that category value)
3. **Embeddings**: use/create a vector-representation of meaningful words in each category's value

Each of these options can be really useful in different scenarios. We're going to focus on one-hot encoding here.

#### One-hot encoding basics

One-hot encoding creates a binary/dummy feature for each category in each categorical feature.

In the example below, the feature **Animal** is split into three binary features &mdash; one for each value in **Animal**. Each binary feature's value is equal to 1 if its respective category value is present in **Animal** for each row. If its category value is not present in the row, the binary feature's value will be 0.

![one-hot-encoding]("C:\Users\mural\Downloads\one-hot-encoding.png")

#### One-hot encoding in Spark MLlib

Even if you understand one-hot encoding, it's important to learn how to perform it using Spark MLlib.

To one-hot encode categorical features in Spark MLlib, we are going to use two classes: [the **`StringIndexer`** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StringIndexer.html#pyspark.ml.feature.StringIndexer) and [the **`OneHotEncoder`** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.OneHotEncoder.html#pyspark.ml.feature.OneHotEncoder).

* The `StringIndexer` class indexes string-type columns to a numerical index. Each unique value in the string-type column is mapped to a unique integer.
* The `OneHotEncoder` class accepts indexed columns and converts them to a one-hot encoded vector-type feature.

#### Applying the `StringIndexer` -> `OneHotEncoder` -> `VectorAssembler`workflow

First, we'll need to index the categorical features of the DataFrame. `StringIndexer` takes a few arguments:

1. A list of categorical columns to index.
2. A list names for the indexed columns being created.
3. Directions for how to handle new categories when transforming data.

Because `StringIndexer` has to learn which categories are present before indexing, it's an **estimator** &mdash; remember that means we need to call its `fit` method. Its result can then be used to transform our data.
