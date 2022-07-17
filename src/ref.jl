function PyRef(x; server::PyServer=default_server())
    if x isa PyRef
        if _server(x) == server
            return x
        else
            error("different server")
        end
    else
        return pyexec(PyRef, ""; locals=(; ans=x), server)
    end
end

_server(x::PyRef) = getfield(x, :server)
_ref(x::PyRef) = getfield(x, :ref)

function _pyref_finalizer(x::PyRef)
    server = _server(x)
    ref = _ref(x)
    # add the ref to the decrefs queue
    @lock server.decrefs_lock push!(server.decrefs, ref)
    # if the server is idle, handle decrefs immediately
    trylock(server) && unlock(server)
    return
end

function Base.show(io::IO, x::PyRef)
    str = pyexec(String, "ans = repr(x)"; locals=(; x), server=_server(x))
    print(io, "PyRef: ", str)
end

function Base.print(io::IO, x::PyRef)
    str = pyexec(String, "ans = str(x)"; locals=(; x), server=_server(x))
    print(io, str)
end

function Base.getproperty(x::PyRef, k::Symbol)
    return pyexec(PyRef, "ans = getattr(x, k)"; locals=(; x, k=String(k)), server=_server(x))
end

function Base.setproperty!(x::PyRef, k::Symbol, v)
    return pyexec(Nothing, "setattr(x, k, v)"; locals=(; x, k=String(k), v), server=_server(x))
end

function Base.propertynames(x::PyRef)
    return pyexec(Vector{Symbol}, "ans = dir(x)"; locals=(; x), server=_server(x))
end
