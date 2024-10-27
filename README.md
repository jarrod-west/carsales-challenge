# carsales-challenge

## Problem Analysis

The primary goal of this challenge is to provide access to data that can be used to assist in predicting payment lapses.  The resulting dataset or datasets will be interpreted by data analysts and data scientists.

Unfortunately, the sample data indicates that there are two key pieces of information missing from the data sources which will make the task difficult:
* There is no indicator of when the invoices are due, only when they were issued
* There is no indicator of when (or if) an invoice was paid

## Assumptions

Based on the problem description and the above analysis I've made the following assumptions:
* That the invoice status (i.e. paid or unpaid) is available to the end user in a separate data stream, and
* The end user is able to include this data along with the datasets

Otherwise, the end user may be viewing the data directly, as a report or dashboard

## Solution

With the following assumptions in mind, I've translated the problem into two functional requirements, and provided a dataset for each.

1. A list of outstanding invoices, including their total amount, ordered by issue date.  The company's contact details are included to assist in notifying them of their upcoming (or lapsed) payments.
1. A list of amount owing by account, ordered by the highest amount, and again with the company's contact details.  This dataset attempts to highlight the businesses with the largest outstanding risk of lapse.

### Code

Generation of the datasets is provided by two functions using PySpark, the python API for Apache Spark.  Both functions can be found under `challenge/invoices.py`, along with some helper functions to generate intermediary datasets (total invoice costs, and invoices with customer details respectively).  Each function assumes that the four incoming datasets have already been loaded into DataFrames - in the example this has been completed by the Spark CSV plugin, but in the production case these could come from other databases, APIs, or elsewhere.

The solution includes code quality and linting using `ruff`, but no unit tests - given the simplicity of the solution and the fact that it mostly consists of operations from the third-party PySpark library it was decided that any tests would be low quality.

### Running the Example

The file at `bin/example.py` provides a way to demonstrate both datasets by:
* Loading the sample data from the CSV files
* Generating both datasets using `challenge/invoices.py`
* Either printing a sample of the results to the screen or writing the entire results to a file

**Pre-requisites**
A system with both `python3` and `pipenv` installed globally - this project was built using python 3.10

**Steps**
* Install dependencies using `pipenv install`
* Open the virtual environment using `pipenv shell`
* Run the example file using `python -m bin.example \[-w\]` where:
  * If `-w` is ommitted it will print a sample of each dataset to the console
  * If `-w` is included it will write both datasets in their entirety under the directory `./out/<dataset-name>_<timestamp>`. The relevant file will be titled `part-<id>.csv`

## Alternatives

If the above limitations did not exist and the goal *was* to predict chance of lapse for a particular invoice I would have a quite different solution, i.e:
* Aggregate the data from all four sources into one large, invoice-based dataset
* Separate out the invoices that have been paid, and group them into at least two categories: paid on time and paid late (though depending on the requirements, I could easily create more categories based on duration of lapse, e.g. paid within a week after due date, within a month, etc.)
* Feed this data into some form of classification model, using a split of the data as training and testing data (most likely multiple times, using cross-validation).
* Take the resulting model and apply it to the invoices that have not yet been paid, then use the results to anticipate potential lapses and proactively contact the account holders before this occurs.