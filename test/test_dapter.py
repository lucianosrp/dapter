import narwhals as nw
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal

from dapter import BaseMapper, accepts, to_lower
from dapter.column_names import accepts_anycases


def test_name_accepts():
    class MyTableMapper(BaseMapper):
        name = accepts("Name", "username")

    m = MyTableMapper()

    df = pl.DataFrame({"name": ["foo", "BAR", "looooooong"]})
    df2 = pd.DataFrame({"username": ["foo", "BAR", "looooooong"]})
    df3 = pl.DataFrame({"Name": ["foo", "BAR", "looooooong"]})

    new_dfs = m.apply(df, df2, df3)
    assert all(["name" in df.columns for df in new_dfs])


def test_modifier_error():
    class MyTableMapper(BaseMapper):
        name = (accepts("Name", "username"), "wrong_modifier")

    with pytest.raises(TypeError):
        MyTableMapper()


def test_accepts_with_extra_columns():
    class MyTableMapper(BaseMapper):
        name = accepts("Name", "username")

    m = MyTableMapper()

    df = pl.DataFrame({"name": ["foo", "BAR", "looooooong"], "ignore": [10, 20, 30]})
    df2 = pl.DataFrame({"username": ["foo", "BAR", "looooooong"]})
    df3 = pl.DataFrame({"Name": ["foo", "BAR", "looooooong"], "ignore": [10, 20, 30]})

    new_dfs = m.apply(df, df2, df3)
    assert all(["name" in df.columns for df in new_dfs])
    assert all(["ignore" not in df.columns for df in new_dfs])


def test_name_accepts_anycases():
    class MyTableMapper(BaseMapper):
        user_name = accepts_anycases()

    m = MyTableMapper()

    df = pd.DataFrame({"user_name": ["foo", "BAR", "looooooong"]})
    df2 = pd.DataFrame({"UserName": ["foo", "BAR", "looooooong"]})
    df3 = pd.DataFrame({"USER_NAME": ["foo", "BAR", "looooooong"]})
    df4 = pd.DataFrame({"User Name": ["foo", "BAR", "looooooong"]})

    new_dfs = m.apply(df, df2, df3, df4)

    assert all(["user_name" in df.columns for df in new_dfs])


def test_str_accessor_function():
    def slice_4(s: nw.Series):
        return s.str.slice(0, 4)

    class MyTableMapper(BaseMapper):
        name = (to_lower, slice_4, accepts("Name", "username"))

    df = pl.DataFrame({"name": ["foo", "BAR", "looooooong"]})
    df2 = pd.DataFrame({"username": ["foo", "BAR", "looooooong"]})
    df3 = pl.DataFrame({"Name": ["foo", "BAR", "looooooong"]})

    m = MyTableMapper()
    new_dfs = m.apply(df, df2, df3)
    for new_df in new_dfs:
        for val in new_df["name"]:
            assert val == val.casefold()
            assert len(val) <= 4


def test_series_modifier():
    def convert_to_eur(s: pd.Series):
        return s * 0.92

    class MyTableMapper(BaseMapper):
        amount_eur = (convert_to_eur, accepts("USD"))

    m = MyTableMapper()
    df = m.apply(pd.DataFrame({"USD": [10, 100]}))
    assert "amount_eur" in df.columns
    assert df.amount_eur.round().tolist() == [9.0, 92.0]
