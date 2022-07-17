function Base.show(io::IO, mime::M, media::PyMedia{M}) where {M<:MIME}
    write(io, media.data)
    return
end
