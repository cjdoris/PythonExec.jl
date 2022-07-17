# PythonExec.jl

Execute Python code from Julia.

## Install

```
pkg> add https://github.com/cjdoris/PythonExec.jl
```

## Usage

The API mainly consists of one function `pyexec` which executes the given piece of Python
code:
```
julia> pyexec("print('hello from python')")
hello from python
```

If the `ans` variable is assigned in the code, then its value will be returned:
```
julia> pyexec("ans = 1 + 2")
3
```

You may specify some input variables with the `locals` argument:
```
julia> pyexec("ans = x + y", locals=(x=10, y=2))
12
```

You may optionally specify a return type to override the default behaviour:
```
julia> pyexec(Symbol, "ans = 'hello'")
:hello
```

Multidimensional buffers and numpy arrays are supported:
```
julia> pyexec("import numpy; ans = numpy.random.randn(2,3)")
2×3 Matrix{Float64}:
  2.23817   -1.02546   0.558285
 -0.901372  -0.227179  1.17542
```

## Packages

By default, [CondaPkg.jl](https://github.com/cjdoris/CondaPkg.jl) is used to install Python
and any packages required. Use `CondaPkg.add()` to add any Python packages you need.
