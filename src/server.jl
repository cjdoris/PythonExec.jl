const SERVER_PY = joinpath(@__DIR__, "server.py")

### SERVER CORE

function PyServer()
    sock = ZMQ.Socket(ZMQ.PAIR)
    address = ""
    ok = false
    for port in 8800:8899
        address = "tcp://127.0.0.1:$port"
        try
            ZMQ.bind(sock, address)
            ok = true
            break
        catch err
            if err isa ZMQ.StateError && err.msg == "Address in use"
                continue
            else
                rethrow()
            end
        end
    end
    ok || error("could not find any free ports")
    cmd = `python $SERVER_PY`
    proc = CondaPkg.withenv() do
        ENV["JULIA_PYTHONEXEC_ADDRESS"] = address
        run(cmd; wait=false)
    end
    return PyServer(sock, proc)
end

function Base.isopen(server::PyServer)
    if server.open
        server.open &= isopen(server.sock)
    end
    return server.open
end

function Base.close(server::PyServer)
    @lock server.lock if isopen(server)
        send(server, (; tag="stop"))
        server.open = false
    end
    return
end

function Base.lock(server::PyServer)
    return lock(server.lock)
end

function Base.trylock(server::PyServer)
    return trylock(server.lock)
end

function Base.unlock(server::PyServer)
    # handle any pending decrefs
    if isopen(server)
        @lock server.decrefs_lock if !isempty(server.decrefs)
            send(server, (; tag="decref", refs=server.decrefs))
            empty!(server.decrefs)
        end
    end
    return unlock(server.lock)
end

function send(py::PyServer, msg)
    line = JSON3.write(msg)
    @debug "send" line
    ZMQ.send(py.sock, line)
    return
end

function recv(py::PyServer)
    line = ZMQ.recv(py.sock, String)
    @debug "recv" line
    return JSON3.read(line)::JSON3.Object
end

### DEFAULT SERVER

const DEFAULT_SERVER = Ref{PyServer}()

function default_server()
    if !isassigned(DEFAULT_SERVER) || !isopen(DEFAULT_SERVER[])
        DEFAULT_SERVER[] = PyServer()
    end
    ans = DEFAULT_SERVER[]
    return ans
end

### PING SERVER

function ping(value::AbstractString="ping"; server::PyServer=default_server())
    @lock server begin
        send(server, (; tag="echo", value))
        while true
            ans = recv(server)
            tag = ans.tag::String
            if tag == "result"
                ans.value::String == value || error("unexpected value: $ans")
                return
            else
                handle_misc(ans; server)
            end
        end
    end
end

### MESSAGE HANDLERS

function handle_misc(msg; server)
    tag = msg.tag::String
    if tag == "error"
        handle_error(msg)
    elseif tag == "stdout"
        print(stdout, msg.text::String)
    elseif tag == "stderr"
        print(stderr, msg.text::String)
    else
        error("unexpected tag: $tag")
    end
end

function handle_error(ans)
    error("PythonExec: $(rstrip(ans.msg))\nPython Stacktrace:\n$(rstrip(ans.tb))")
end

### DESERIALIZE ANY

get_format(::Type{Any}) = "any"

function deserialize(::Type{Any}, val, server)
    if val isa JSON3.Object
        t = val.type::String
        if t == "int"
            return _deserialize_int(val, server)
        elseif t == "list"
            return _deserialize_list(val, server)
        elseif t == "tuple"
            return _deserialize_tuple(val, server)
        elseif t == "dict"
            return _deserialize_dict(val, server)
        elseif t == "bytes"
            return _deserialize_bytes(val, server)
        elseif t == "set"
            return _deserialize_set(val, server)
        elseif t == "ref"
            return _deserialize_ref(val, server)
        elseif t in ("buffer", "ndarray")
            return deserialize(Array, val, server)
        else
            error("unexpected type: $t")
        end
    else
        return val
    end
end

function _deserialize_list(::Type{T}, val, server) where {T}
    v = val.val::JSON3.Array
    return [deserialize(T, x, server) for x in v]::Vector
end

_deserialize_list(val, server) = _deserialize_list(Any, val, server)

function _deserialize_dict(::Type{K}, ::Type{V}, val, server) where {K,V}
    return Dict(deserialize(K, k, server) => deserialize(V, v, server) for (k, v) in val.val::JSON3.Array)
end

_deserialize_dict(val, server) = _deserialize_dict(Any, Any, val, server)

function _deserialize_bytes(val, server)
    return Base64.base64decode(val.val::String)
end

function _deserialize_set(::Type{T}, val, server) where {T}
    return Set(deserialize(T, x, server) for x in val.val::JSON3.Array)
end

_deserialize_set(val, server) = _deserialize_set(Any, val, server)

### DESERIALIZE REF

get_format(::Type{PyRef}) = "ref"

_deserialize_ref(val, server) = PyRef(Val(:new), server, val.val::String)

deserialize(::Type{PyRef}, val, server) = _deserialize_ref(val, server)

### DESERIALIZE NONE

get_format(::Type{Nothing}) = "none"

_deserialize_none(val, server) = nothing

deserialize(::Type{Nothing}, val, server) = _deserialize_none(val, server)

### DESERIALIZE BOOL

get_format(::Type{Bool}) = "bool"

_deserialize_bool(val, server) = val.val::Bool

deserialize(::Type{Bool}, val, server) = _deserialize_bool(val, server)

### DESERIALIZE INT

get_format(::Type{<:Integer}) = "int"

function _deserialize_int(val, server)
    if val isa JSON3.Object
        v = val.val::String
        x = tryparse(Int, v)
        if x === nothing
            return parse(BigInt, v)
        else
            return x
        end
    else
        return val::Int
    end
end

deserialize(::Type{T}, val, server) where {T<:Integer} = convert(T, _deserialize_int(val, server))::T

### DESERIALIZE FLOAT

get_format(::Type{<:AbstractFloat}) = "float"

function _deserialize_float(val, server)
    if val isa Float64
        return val
    elseif val isa Int
        return convert(Float64, val)
    else
        return convert(Float64, val)::Float64
    end
end

deserialize(::Type{T}, val, server) where {T<:AbstractFloat} = convert(T, _deserialize_float(val, server))::T

### DESERIALIZE STR

get_format(::Type{<:AbstractString}) = "str"
get_format(::Type{Symbol}) = "str"

_deserialize_str(val, server) = val::String

deserialize(::Type{T}, val, server) where {T<:AbstractString} = convert(T, _deserialize_str(val, server))::T
deserialize(::Type{Symbol}, val, server) = Symbol(_deserialize_str(val, server))

### DESERIALIZE BUFFER

get_format(::Type{PyBuffer}) = "buffer"

function _deserialize_buffer(val, server)
    buf = PyBuffer(
        val.format,
        val.itemsize,
        val.shape,
        Base64.base64decode(val.data),
    )
    val.ndim == length(buf.shape) || error("length(shape)=$(length(buf.shape)) but ndim=$(val.ndim)")
    val.nbytes == length(buf.data) || error("length(data)=$(length(buf.data)) but nbytes=$(val.nbytes)")
    return buf
end

deserialize(::Type{PyBuffer}, val, server) = _deserialize_buffer(val, server)

### DESERIALIZE ARRAY

get_format(::Type{Array}) = ("array", "any", nothing)
get_format(::Type{Array{T}}) where {T} = ("array", get_format(T), nothing)
get_format(::Type{Array{T,N} where {T}}) where {N} = ("array", "any", N)
get_format(::Type{Array{T,N}}) where {T,N} = ("array", get_format(T), N)

_ndims(::Type{<:Array{T,N} where T}) where {N} = N
_ndims(::Type{<:Array}) = nothing

_eltype(::Type{<:Array{T}}) where {T} = T
_eltype(::Type{<:Array}) = nothing

function deserialize(::Type{A}, val, server) where {A<:Array}
    T = _eltype(A)
    N = _ndims(A)
    tp = val.type::String
    if tp == "list"
        if N === nothing || N == 1
            if T === nothing
                return [deserialize(Any, x, server) for x in val.val::JSON3.Array]::A
            else
                return T[deserialize(T, x, server) for x in val.val::JSON3.Array]::A
            end
        else
            @assert false
        end
    elseif tp == "buffer"
        T2 = buffer_format_to_type(val.format::String)
        sizeof(T2) == val.itemsize::Int || error("sizeof($T2)==$(sizeof(T2)) but itemsize=$(val.itemsize)")
        _N = val.ndim::Int
        if N === nothing
            N2 = _N
        else
            N == _N || error("want ndims=$N but array has ndims=$_N")
            N2 = N
        end
        sz = NTuple{N2,Int}(val.shape)::NTuple{N2,Int}
        return A(reshape(reinterpret(T2, Base64.base64decode(val.data::String)), sz))::A
    elseif tp == "ndarray"
        T2 = dtype_to_type(val.dtype::String)
        _N = val.ndim::Int
        if N === nothing
            N2 = _N
        else
            N == _N || error("want ndims=$N but array has ndims=$_N")
            N2 = N
        end
        sz = NTuple{N2,Int}(val.shape)::NTuple{N2,Int}
        return A(reshape(reinterpret(T2, Base64.base64decode(val.data::String)), sz))::A
    else
        @assert false
    end
end

function buffer_format_to_type(fmt)
    if fmt == "b"
        return Cchar
    elseif fmt == "B"
        return Cuchar
    elseif fmt == "h"
        return Cshort
    elseif fmt == "H"
        return Cushort
    elseif fmt == "i"
        return Cint
    elseif fmt == "I"
        return Cuint
    elseif fmt == "l"
        return Clong
    elseif fmt == "L"
        return Culong
    elseif fmt == "q"
        return Clonglong
    elseif fmt == "Q"
        return Culonglong
    elseif fmt == "e"
        return Float16
    elseif fmt == "f"
        return Cfloat
    elseif fmt == "d"
        return Cdouble
    elseif fmt == "Ze"
        return Complex{Float16}
    elseif fmt == "Zf"
        return Complex{Cfloat}
    elseif fmt == "Zd"
        return Complex{Cdouble}
    elseif fmt == "?"
        return Bool
    elseif fmt == "P"
        return Ptr{Cvoid}
    else
        error("unknown buffer format: $fmt")
    end
end

function dtype_to_type(dt)
    if dt isa String
        # check byte-order
        oc = dt[1]
        if oc in "<>"
            if oc != (Base.ENDIAN_BOM == 0x04030201 ? '<' : '>')
                error("unsupported: byte-swapped dtype=$dt")
            end
        elseif oc != '|'
            error("unsupported order char $oc in dtype=$dt")
        end
        # parse type
        tc = dt[2]
        sz = dt[3:end]
        if tc == 'f'
            if sz == "2"
                return Float16
            elseif sz == "4"
                return Float32
            elseif sz == "8"
                return Float64
            end
        elseif tc == 'i'
            if sz == "1"
                return Int8
            elseif sz == "2"
                return Int16
            elseif sz == "4"
                return Int32
            elseif sz == "8"
                return Int64
            end
        elseif tc == 'u'
            if sz == "1"
                return UInt8
            elseif sz == "2"
                return UInt16
            elseif sz == "4"
                return UInt32
            elseif sz == "8"
                return UInt64
            end
        elseif tc == 'c'
            if sz == "4"
                return ComplexF16
            elseif sz == "8"
                return ComplexF32
            elseif sz == "16"
                return ComplexF64
            end
        elseif tc == 'b'
            if sizeof(Bool) == 1 && sz == "1"
                return Bool
            end
        end
    end
    error("unsupported dtype: $dt")
end

### DESERIALIZE TUPLE

get_format(::Type{Tuple}) = "tuple"
get_format(::Type{T}) where {T<:Tuple} = ("tuple", map(format, T.parameters)...)

function _deserialize_tuple(::Type{Tuple}, val, server)
    v = val.val::JSON3.Array
    return Tuple(deserialize(Any, x, server) for x in v)::Tuple
end

function _deserialize_tuple(::Type{T}, val, server) where {T<:Tuple}
    v = val.val::JSON3.Array
    return T(deserialize(t, x, server) for (t, x) in zip(T.parameters, v))::T
end

_deserialize_tuple(val, server) = _deserialize_tuple(Tuple, val, server)

deserialize(::Type{T}, val, server) where {T<:Tuple} = _deserialize_tuple(T, val, server)

### DESERIALIZE MEDIA

get_format(::Type{PyMedia{MIME{M}}}) where {M} = ("media", string(M))

function deserialize(::Type{PyMedia{M}}, val, server) where {M}
    data = Base64.base64decode(val)
    return PyMedia{M}(data)
end

### DESERIALIZE FALLBACK

function get_format(::Type{T}) where {T}
    if T isa Union
        return ("union", get_format(T.a), get_format(T.b))
    else
        error("cannot coerce Python objects to $T")
    end
end

function _deserialize_union(::Type{T}, val, server) where {T}
    idx = val.idx::Int
    if idx == 0
        return deserialize(T.a, val.val, server)::T
    elseif idx == 1
        return deserialize(T.b, val.val, server)::T
    end
    @assert false
end

function deserialize(::Type{T}, val, server) where {T}
    if T isa Union
        return _deserialize_union(T, val, server)
    else
        @assert false
    end
end

### SERIALIZE

serialize(x::PyRef) = (; type="ref", val=_ref(x))
serialize(x::Nothing) = x
serialize(x::AbstractString) = convert(String, x)
serialize(x::Bool) = x
serialize(x::Integer) = abs(x) < (1<<30) ? convert(Int, x) : (; type="int", val=string(convert(BigInt, x)))
serialize(x::Real) = convert(Float64, x)
serialize(x::AbstractVector) = (; type="list", val=[serialize(x) for x in x])
serialize(x::AbstractSet) = (; type="set", val=[serialize(x) for x in x])
serialize(x::AbstractDict) = (; type="dict", val=[(serialize(k), serialize(v)) for (k, v) in pairs(x)])
serialize(x::Tuple) = (; type="tuple", val=[serialize(x) for x in x])

### EXEC

function pyexec(
    ::Type{T},
    code::AbstractString;
    server::PyServer=default_server(),
    scope::Union{AbstractString,Nothing}=nothing,
    locals=NamedTuple(),
) where {T}
    format = get_format(T)
    orig_locals = locals
    if locals !== nothing
        locals = Dict(k=>serialize(v) for (k,v) in (locals isa NamedTuple ? pairs(locals) : locals))
    end
    GC.@preserve orig_locals @lock server begin
        send(server, (; tag="exec", code, scope, locals, format))
        while true
            ans = recv(server)
            tag = ans.tag::String
            if tag == "result"
                if T == Nothing
                    return nothing
                else
                    return deserialize(T, ans.value, server)
                end
            else
                handle_misc(ans; server)
            end
        end
    end
end

pyexec(code::AbstractString; kw...) = pyexec(Any, code; kw...)
