# What is Dapter ?
> Data + Adapter


## TL;DR

Dapter is a convenient tool that helps working with multiple data sources. It allows you to easily rename column names and transform your data.

### Sample code

!!! info
    This is  a quick example, if it is not too clear yet, continue reading this article

=== "Pandas"

    ```python exec="1" source="above"
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
                "amount_usd": 2000,
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

    class TransactionMapper(BaseMapper):
        transaction_date = accepts("transaction_date", "Date", "DATE")
        vendor_name = accepts_anycases()
        amount_eur = (accepts("Amount USD", "amount_usd", "USD"), convert_to_eur)
        category = accepts("Category", "category", "CAT")

    mapper = TransactionMapper()

    dfs = mapper.apply(df1, df2, df3)
    df = pd.concat(dfs)
    df
    print(df.to_markdown(index=False)) # markdown-exec: hide

    ```

=== "Polars"

    ```python exec="1" source="above"
    from dapter import BaseMapper, accepts, accepts_anycases
    import polars as pl


    df1 = pl.DataFrame(
        [
            {
                "Date": "2023-02-01 10:00:01",
                "Vendor Name": "Golden Oil LLC",
                "Amount USD": 49.99,
                "Category": "Personal",
            }
        ]
    )

    df2 = pl.DataFrame(
        [
            {
                "transaction_date": "2023-03-01 10:00:01",
                "vendor_name": "Get Cars Inc.",
                "amount_usd": 2000,
                "category": "Transportation",
            }
        ]
    )
    df3 = pl.DataFrame(
        [
            {
                "DATE": "2023-04-01 10:00:01",
                "VENDOR_NAME": "Maintainers Exc.",
                "USD": 5249.0,
                "CAT": "Personal",
            }
        ]
    )


    def convert_to_eur(col: pl.Series) -> pl.Series:
        return col * 0.92

    class TransactionMapper(BaseMapper):
        transaction_date = accepts("transaction_date", "Date", "DATE")
        vendor_name = accepts_anycases()
        amount_eur = (accepts("Amount USD", "amount_usd", "USD"), convert_to_eur)
        category = accepts("Category", "category", "CAT")

    mapper = TransactionMapper()

    dfs = mapper.apply(df1, df2, df3)
    df = pl.concat(dfs)
    df
    print(df.to_pandas().to_markdown(index=False)) # markdown-exec: hide
    ```


## In more details

Ever worked with multiple data sources that all have the same type of data?


Sometimes you may want to load and convert the different sets (DataFrames) to a unique format so that all other analysis can be done at once.

Let's say we have 3 transaction records from 3 different banks.
All three have the same data (transactions), but they just define each header in different ways:

**Bank A**

| Date | Vendor Name | Amount USD| Category |
|------|--------|------------|---------|
2023-02-01 10:00:01| Golden Oil LLC | 49.99  |  Personal


**Bank B**

| transaction_date | vendor_name | amount_usd| category |
|------|--------|------------|----------|
2023-03-01 10:00:01| Get Cars Inc. | 2999.9  |  Transportation

**Bank C**

| DATE | VENDOR_NAME |USD| CAT |
|------|--------|------------|----------|
2023-04-01 10:00:01| Maintainers Exc. | 5249.0  |  Personal

### The old way

We now want to convert the amount paid in another currency. We could use a function to create a new column.

```python
def convert_amount(amount:pd.Series, rate:float = 0.92) -> pd.Series:
    return amount * rate
```

There is nothing wrong by doing so, the issue we now have is that every DataFrame will have a different column name: ("Amount USD", "amount_usd", "USD")

We could iterate over the sources and pass a custom column name to get the right Series:

```python
for df, amount_name in zip((bank_a, bank_b, bank_c), ("Amount USD", "amount_usd", "USD")):
    df["converted"] = convert_amount(df[amount_name])

```
OR, we could have just converted the column names to a unique format when first loading the data so that we could just refer to "amount_usd" once.

```python
bank_a = bank_a.rename(columns={"Amount USD":"amount_usd"})
# bank_b already has "amount_usd"
bank_c = bank_c.rename(columns={"USD":"amount_usd"})

for df in (bank_a, bank_b, bank_c):
    df["converted"] = convert_amount(df["amount_usd"])
```

So there are really just two ways to handle multiple sources at once:

1. Load the data as-is and handle each transformation considering the different field names

2. Load the data and convert (harmonize) the headers so that every transformation there after can be streamlined.

`dapter` handles the second point.
You can now start planning on how to process your data even before knowing what would be the final column names. As long as the data providers would deliver the same types, column names conversion can easily be handled with `dapter`.

### Using Dapter
Step 1: define a "Mapper".

This custom class will handle the various conversions. Each class attribute will correspond to a column name. In our case we can define a mapper like so:
```python
from dapter import BaseMapper, accepts

class TransactionMapper(BaseMapper):
    transaction_date = accepts("transaction_date", "Date", "DATE")
    vendor_name = accepts("Vendor Name", "vendor_name", "VENDOR_NAME")
    amount_usd = accepts("Amount USD", "amount_usd", "USD")
    category = accepts("Category", "category", "CAT")


mapper = TransactionMapper()
```
Step 2:

We can now convert all DataFrames in one go:

```python
bank_a, bank_b, bank_c = mapper.apply(bank_a, bank_b, bank_c)
```

Now we can be sure that all transaction data have the same column names as the attribute names we have assign, since the function `accepts` will map every argument to the column name.

In the background it would be something similar to:

```python
 bank_a.rename(columns={"Amount":"amount_usd","USD":"amount_usd"})
```

For column names that happens to have the same name but just in a different case, we can use `accept_anycase`

```python
class TransactionMapper(BaseMapper):
    transaction_date = accepts("transaction_date", "Date", "DATE")
    vendor_name = accepts_anycases()
    amount_eur = (accepts("Amount USD", "amount_usd", "USD"), convert_to_eur)
    category = accepts("Category", "category", "CAT")
```

We can even apply transformations for any specific column as such:

1 - Define a function that accepts a series. Similar to the first examples above:

```python
def convert_to_eur(col: pd.Series) -> pd.Series:
    return col * 0.92

```

2 - Pass this function directly into our mapper model:
```python
class TransactionMapper(BaseMapper):
    transaction_date = accepts("transaction_date", "Date", "DATE")
    vendor_name = accepts_anycases()
    amount_eur = (accepts("Amount USD", "amount_usd", "USD"), convert_to_eur)
    category = accepts("Category", "category", "CAT")

```
Let's see what would be the output

```python
m  = TransactionMapper()
m.apply(df)
```

**Before**

| Date | Vendor Name | Amount USD| Category |
|------|--------|------------|---------|
2023-02-01 10:00:01| Golden Oil LLC | 49.99  |  Personal

**After**

| transaction_date | vendor_name | amount_eur | category |
|------|--------|------------|---------|
2023-02-01 10:00:01| Golden Oil LLC | 45.99  |  Personal
