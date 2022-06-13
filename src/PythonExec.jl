module PythonExec

import Base64
import CondaPkg
import JSON3

export PyServer, PyBuffer, pyexec

const SERVER_PY = joinpath(@__DIR__, "server.py")

mutable struct PyServer
    proc::Base.Process
    open::Bool
    function PyServer(proc::Base.Process)
        finalizer(close, new(proc, true))
    end
end

struct PyBuffer
    format::String
    itemsize::Int
    nbytes::Int
    ndim::Int
    shape::Vector{Int}
    data::Vector{UInt8}
end

function PyServer()
    cmd = `python $SERVER_PY`
    proc = CondaPkg.withenv() do
        open(cmd, "r+")
    end
    return PyServer(proc)
end

function send(py::PyServer, msg)
    io = py.proc
    JSON3.write(io, msg)
    println(io)
    flush(io)
    return
end

function recv(py::PyServer)
    line = readline(py.proc)
    return JSON3.read(line)::JSON3.Object
end

function ping(py::PyServer, msg::AbstractString="ping")
    send(py, (tag="echo", msg=msg))
    while true
        ans = recv(py)
        tag = ans.tag::String
        if tag == "result"
            ans.value::String == msg || error("unexpected value: $ans")
            return
        elseif tag == "error"
            handle_error(ans)
        else
            error("unexpected tag: $ans")
        end
    end
end

function handle_error(ans)
    error("PythonExec: $(rstrip(ans.msg))\nPython Stacktrace:\n$(rstrip(ans.tb))")
end

type_to_format(::Type{Any}) = "any"
type_to_format(::Type{Nothing}) = "none"
type_to_format(::Type{Bool}) = "bool"
type_to_format(::Type{<:Integer}) = "int"
type_to_format(::Type{<:AbstractFloat}) = "float"
type_to_format(::Type{<:AbstractString}) = "str"
type_to_format(::Type{PyBuffer}) = "buffer"
type_to_format(::Type{Array}) = ("array", "any", nothing)
type_to_format(::Type{Array{T}}) where {T} = ("array", type_to_format(T), nothing)
type_to_format(::Type{Array{T,N} where {T}}) where {N} = ("array", "any", N)
type_to_format(::Type{Array{T,N}}) where {T,N} = ("array", type_to_format(T), N)
type_to_format(::Type{Tuple}) = "tuple"
type_to_format(::Type{T}) where {T<:Tuple} = ("tuple", map(type_to_format, T.parameters)...)
function type_to_format(::Type{T}) where {T}
    if T isa Union
        return ("union", type_to_format(T.a), type_to_format(T.b))
    else
        error("cannot coerce Python objects to $T")
    end
end

function handle_value(::Type{T}, val) where {T}
    if T isa Union
        idx = val.idx::Int
        if idx == 0
            return handle_value(T.a, val.val)::T
        elseif idx == 1
            return handle_value(T.b, val.val)::T
        else
            error("not possible")
        end
    else
        return convert(T, val)::T
    end
end

function handle_value(::Type{Any}, val)
    if val isa JSON3.Object
        t = val.type::String
        if t == "int"
            let x = tryparse(Int, val.val::String)
                return x === nothing ? parse(BigInt, val.val::String) : x
            end
        elseif t == "list"
            return [handle_value(Any, x) for x in val.val]
        elseif t == "tuple"
            return Tuple(handle_value(Any, x) for x in val.val)
        elseif t == "dict"
            return Dict(handle_value(Any, k) => handle_value(Any, v) for (k,v) in val.val)
        elseif t == "bytes"
            return Base64.base64decode(val.val::String)
        elseif t == "set"
            return Set(handle_value(Any, x) for x in val.val)
        else
            error("unexpected type: $t")
        end
    else
        return val
    end
end

function handle_value(::Type{T}, val) where {T<:Integer}
    if val isa String
        return parse(T, val)::T
    else
        return convert(T, val)::T
    end
end


function handle_value(::Type{Tuple}, val)
    return Tuple(handle_value(Any, x) for x in val)
end

function handle_value(::Type{T}, val) where {T<:Tuple}
    return T(handle_value(t, x) for (t, x) in zip(T.parameters, val))
end

function handle_value(::Type{PyBuffer}, val)
    return PyBuffer(
        val.format,
        val.itemsize,
        val.nbytes,
        val.ndim,
        val.shape,
        Base64.base64decode(val.data),
    )
end

_ndims(::Type{<:Array{T,N} where T}) where {N} = N
_ndims(::Type{<:Array}) = nothing

_eltype(::Type{<:Array{T}}) where {T} = T
_eltype(::Type{<:Array}) = nothing

function handle_value(::Type{A}, val) where {A<:Array}
    T = _eltype(A)
    N = _ndims(A)
    if val isa AbstractVector
        if N === nothing || N == 1
            if T === nothing
                return [handle_value(Any, x) for x in val]
            else
                return T[handle_value(T, x) for x in val]
            end
        elseif N > 1
            slices = [handle_value(T===nothing ? Array{T,N-1} where T : Array{T,N-1}, x) for x in val]
            # TODO: how to cat without the splat?
            return cat(slices..., dims=N)
        else
            error("cannot happen")
        end
    else
        error("cannot happen")
    end
end

function pyexec(::Type{T}, py::PyServer, code::AbstractString; scope=nothing, locals=NamedTuple()) where {T}
    format = type_to_format(T)
    send(py, (; tag="exec", code, scope, locals, format))
    while true
        ans = recv(py)
        tag = ans.tag::String
        if tag == "result"
            if T == Nothing
                return
            else
                return handle_value(T, ans.value)
            end
        elseif tag == "error"
            handle_error(ans)
        else
            error("unexpected tag: $ans")
        end
    end
end

pyexec(py::PyServer, code::AbstractString; kw...) = pyexec(Any, py, code; kw...)

function Base.close(py::PyServer)
    if py.open
        send(py, (; tag="stop"))
        py.open = false
    end
    return
end

end # module PythonExec
