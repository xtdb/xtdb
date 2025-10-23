---
title: Numeric functions
---

:::note
- If any input expression is null, the result will also be null.
- If all arguments are integers, the result will also be an integer; otherwise, all arguments will be cast to floating-point values before applying the function.
  Particularly, the division function performs integer division if it's only given integer values.
- If the result would under-/overflow the widest type of the input arguments, a runtime exception will be thrown.
- Trying to divide by zero will result in a runtime exception.
:::

## Basic arithmetic functions

The standard arithmetic functions are available:

- `expr1 + expr2` (addition)
- `expr1 - expr2` (subtraction)
- `expr1 * expr2` (multiplication)
- `expr1 / expr2` (division)

## Other numeric functions

`ABS(x)`
: absolute value of `x`

`CEIL(x)` | `CEILING(x)`
: nearest integer greater than or equal to `x`

`EXP(x)`
: ℯ (base of natural logarithms) raised to the power of `x`

`FLOOR(x)`
: nearest integer less than or equal to `x`

`LN(x)`
: natural logarithm

`LOG(x, y)`
: logarithm of `x`, base `y`

`LOG10(x)`
: logarithm of `x`, base 10

`MOD(x, y)`
: modulus of `x`, base `y`

`POWER(x, y)`
: `x` raised to the \`y\`th power

`ROUND(x)` | `ROUND(x, s)`
: rounds `x` to the nearest integer, or to `s` decimal places if specified. When exactly halfway between two values, rounds away from zero (HALF_UP). Supports negative `s` to round to the left of the decimal point.
  - `ROUND(42.5)` → `43.0`
  - `ROUND(42.4382, 2)` → `42.44`
  - `ROUND(1234.56, -1)` → `1230.0`

`SQRT(x)`
: square root

## Trigonometric functions

- `ACOS(x)` (inverse cosine)
- `ASIN(x)` (inverse sine)
- `ATAN(x)` (inverse tangent)
- `COS(x)` (cosine)
- `COSH(x)` (hyperbolic cosine)
- `SIN(x)` (sine)
- `SINH(x)` (hyperbolic sine)
- `TAN(x)` (tangent)
- `TANH(x)` (hyperbolic tangent)

:::note
- Arguments and results in radians
:::
