from __future__ import annotations
from abc import ABC
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Self, Type, TypeVar, get_type_hints

import narwhals.stable.v1 as nw
from narwhals.typing import Frame, IntoDataFrame, IntoFrame

from dapter.column_names import AnyCaseNames, BaseNameModifier, NameAccepts, accepts


@dataclass
class BaseMapper(ABC):
    _expr_modifiers: dict[str, list[Callable[..., nw.Expr]]] | None = None
    _expr_name_modifiers: dict[str, Any] | None = None
    _cols: list[nw.Expr] | None = None

    def __post_init__(self) -> None:
        if self._expr_modifiers is None:
            self._expr_modifiers = {}

        if self._expr_name_modifiers is None:
            self._expr_name_modifiers = {}

        if self._cols is None:
            self._cols = []

        self._get_attrs_values()
        self._apply_exprs()

    def _get_attrs_values(self) -> None:
        assert self._expr_modifiers is not None
        assert self._expr_name_modifiers is not None
        assert self._cols is not None
        for name, modifiers in vars(self.__class__).items():
            if not name.startswith("_"):
                self._expr_modifiers[name] = []
                self._expr_name_modifiers[name] = []

                if not isinstance(modifiers, Iterable):
                    modifiers = (modifiers,)

                for modifier in modifiers:
                    if isinstance(modifier, BaseNameModifier):
                        self._expr_name_modifiers[name].append(modifier)
                    elif isinstance(modifier, Callable):  # type: ignore [arg-type]
                        self._expr_modifiers[name].append(modifier)
                    else:
                        raise TypeError(
                            "Column modifier can only be a callable or an instance of BaseNameModifier"
                        )

    def _apply_exprs(self) -> None:
        assert self._expr_modifiers is not None
        assert self._cols is not None
        for col, modifiers in self._expr_modifiers.items():
            new_col = nw.col(col)
            for modifier in modifiers:
                new_col = modifier(new_col).alias(col)

            self._cols.append(new_col)

    def _rename(self, df: Frame) -> Frame:
        rename_dict = {}
        original_cols = df.columns
        assert self._expr_name_modifiers is not None
        for col, name_modifiers in self._expr_name_modifiers.items():
            for name_modifier in name_modifiers:
                if isinstance(name_modifier, AnyCaseNames):
                    name_modifier.names = (col,)
                for name in name_modifier.get_names():
                    if name in original_cols:
                        rename_dict[name] = col

        df = df.rename(rename_dict)
        return df

    def _apply(
        self,
        df: IntoFrame,
    ) -> Frame:
        df = nw.from_native(df)
        df = self._rename(df)
        return df.select(*self._cols)  # type: ignore [misc]

    def apply(
        self,
        *dfs: IntoFrame,
    ) -> Any | tuple[Any]:
        if len(dfs) == 1:
            return nw.to_native(self._apply(dfs[0]))
        else:
            return tuple(self.apply(df) for df in dfs)
