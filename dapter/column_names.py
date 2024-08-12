from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TypeVar


class BaseNameModifier(ABC):
    def __init__(self, *names: str):
        self.names = names

    @abstractmethod
    def get_names(self) -> list[str]: ...


def assert_snake_case(s: str) -> bool:
    splitted_s = s.split("_")
    assert len(splitted_s) > 0
    for substr in splitted_s:
        assert substr.casefold() == substr


def to_camel_case(s: str) -> str:
    assert_snake_case(s)
    return "".join([i.title() for i in s.split("_")])


def to_screaming_snake_case(s: str) -> str:
    assert_snake_case(s)
    return s.upper()


def to_spaced_words(s: str) -> str:
    assert_snake_case(s)
    return s.replace("_", " ").title()


class NameAccepts(BaseNameModifier):
    def get_names(self) -> list[str]:
        return list(self.names)


class AnyCaseNames(BaseNameModifier):
    def get_names(self) -> list[str]:
        new_names = []
        for name in self.names:
            for func in (to_camel_case, to_spaced_words, to_screaming_snake_case):
                new_names.append(func(name))
        return new_names


def accepts(*names: str) -> NameAccepts:
    return NameAccepts(*names)


def accepts_anycases(*names: str) -> AnyCaseNames:
    return AnyCaseNames(*names)
