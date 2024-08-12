<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/dapter-light.svg">
  <img src="docs/assets/dapter-dark.svg"  width="300">
</picture>


> Data + Adapter 

Dapter is a convenient tool that helps working with multiple data sources. It allows you to easily rename column names and transform your data in one go.

With Dapter, you can store a series of instructions for your data cleaning routines into custom objects. You can then reuse the object to any DataFrames at any part of your code. See the step-by-step example below.


## 📝 Example

Renaming columns and adding transformations can be "lazily" set-up in a tuple:

```python
import pandas as pd
from dapter import accepts

def convert_to_eur(col: pd.Series) -> pd.Series:
    return col * 0.92

eur_col = (accepts("Amount USD", "amount_usd","USD"), convert_to_eur)
```

`euro_col` is a series of instructions that will tell `dapter` to:
- Consider any column that is named after one of the names in `accepts`
- Apply `convert_to_eur` to those columns

Once we have defined all the column _"instructions"_ we can then store them together in a custom object that inherits from `dapter.BaseMapper`

```python
from dapter import BaseMapper

class TransactionMapper(BaseMapper):
    amount_eur = euro_col
```

We have just defined that all instructions of `euro_col` will be assigned to a new column called `amount_eur`.

This object can then be used to apply all the renaming and transformations stored inside it to any `DataFrame`

```python
mapper = TransactionMapper()

dfs = mapper.apply(df1, df2, df3)
df = pd.concat(dfs)
```
## 🧰 Installation

Using pip:

```
pip install dapter
```

## 🔄 Infinite DataFrame compatibility

Dapter uses [narwhals](https://narwhals-dev.github.io/narwhals/) in the background so it can accepts any (See supported[^1]) kind of DataFrame libraries.

Which means you can define Polars `Series` and `Expr` transformations for pandas' `Series` and vice-versa! 

You can also feed any DataFrame to the `apply` method.


[^1]:  cuDF, Modin, pandas, Polars, PyArrow, Dask, Ibis, Vaex

## Full sample code 

```python
from dapter import BaseMapper, accepts, accepts_anycases
import pandas as pd

df1 = pd.DataFrame(
    [
        {
            "Date": "2023-02-01 10:00:01",
            "Vendor Name": "Golden Oil LLC",
            "Amount USD": 49.99,
            "Category": "Personal",
        }
    ]
)

df2 = pd.DataFrame(
    [
        {
            "transaction_date": "2023-03-01 10:00:01",
            "vendor_name": "Get Cars Inc.",
            "amount_usd": 2999.9,
            "category": "Transportation",
        }
    ]
)
df3 = pd.DataFrame(
    [
        {
            "DATE": "2023-04-01 10:00:01",
            "VENDOR_NAME": "Maintainers Exc.",
            "USD": 5249.0,
            "CAT": "Personal",
        }
    ]
)


def convert_to_eur(col: pd.Series) -> pd.Series:
    return col * 0.92

def clean_str(col:pd.Series) -> pd.Series:
    return col.str.to_lower().str.replace(" ","_")

class TransactionMapper(BaseMapper):
    transaction_date = accepts("transaction_date", "Date","DATE")
    vendor_name = accepts_anycases()    
    amount_eur = accepts("Amount USD", "amount_usd","USD"), convert_to_eur
    category = accepts("Category", "category","CAT"), clean_str

mapper = TransactionMapper()

dfs = mapper.apply(df1, df2, df3)
df = pd.concat(dfs)
df
```

| transaction_date | vendor_name | amount_eur | category |
|------|--------|------------|---------|
2023-02-01 10:00:01| Golden Oil LLC | 45.99  |  personal 
2023-03-01 10:00:01| Get Cars Inc. | 2999.9  |  transportation 
2023-04-01 10:00:01| Maintainers Exc. | 5249.0  |  personal 