mutable struct PyServer
    lock::ReentrantLock
    sock::ZMQ.Socket
    proc::Base.Process
    open::Bool
    decrefs::Vector{String}
    decrefs_lock::ReentrantLock
    function PyServer(sock::ZMQ.Socket, proc::Base.Process)
        finalizer(close, new(ReentrantLock(), sock, proc, true, String[], ReentrantLock()))
    end
end

mutable struct PyRef
    server::PyServer
    ref::String
    function PyRef(::Val{:new}, server::PyServer, ref::String)
        # TODO: finalizer (need server/client to work asynchronously)
        return finalizer(_pyref_finalizer, new(server, ref))
    end
end

struct PyBuffer
    format::String
    itemsize::Int
    shape::Vector{Int}
    data::Vector{UInt8}
end

struct PyMedia{M<:MIME}
    data::Vector{UInt8}
end

const PyPNG = PyMedia{MIME"image/png"}
const PyHTML = PyMedia{MIME"text/html"}
const PyJPEG = PyMedia{MIME"image/jpeg"}
const PyTIFF = PyMedia{MIME"image/tiff"}
const PySVG = PyMedia{MIME"image/svg+xml"}
const PyPDF = PyMedia{MIME"application/pdf"}
