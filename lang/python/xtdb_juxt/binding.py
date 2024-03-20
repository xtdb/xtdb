from typing import List

from .expr import Sym
from .types import Expr, ToBindings


class Binding:
    def __init__(self, binding: str, expr_: Expr):
        self.binding = binding
        self.expr = expr_

    def to_json(self):
        return {self.binding: self.expr.to_json()}


def to_bindings(b: ToBindings) -> List[Binding]:
    if isinstance(b, str):
        return [Binding(b, Sym(b))]

    return [Binding(binding, expr_) for binding, expr_ in b.items()]
