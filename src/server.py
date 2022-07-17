import base64
import json
import io
import sys
import time
import traceback
import zmq
import os
from numbers import Real, Integral

DEFAULT_MODULE = '__main__.userspace'

userspace = sys.modules[DEFAULT_MODULE] = type(sys)(DEFAULT_MODULE)

address = os.environ['JULIA_PYTHONEXEC_ADDRESS']
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect(address)

def send(msg):
    socket.send(json.dumps(msg).encode('utf8'))

def recv():
    return json.loads(socket.recv().decode('utf8'))

REFS = {}
REFCOUNTS = {}

def save_ref(x):
    ref = str(id(x))
    if ref in REFS:
        REFCOUNTS[ref] += 1
    else:
        REFS[ref] = x
        REFCOUNTS[ref] = 1
    return ref

def del_ref(ref):
    n = REFCOUNTS[ref]
    if n == 1:
        del REFCOUNTS[ref]
        del REFS[ref]
    elif n > 1:
        REFCOUNTS[ref] = n-1
    else:
        raise ValueError('refcount is negative')

def deserialize(val):
    if type(val) is dict:
        t = val['type']
        if t == 'ref':
            return REFS[val['val']]
        elif t == 'set':
            return {deserialize(v) for v in val['val']}
        elif t == 'dict':
            return {deserialize(k):deserialize(v) for (k,v) in val['val']}
        elif t == 'tuple':
            return tuple(deserialize(v) for v in val['val'])
        elif t == 'list':
            return [deserialize(v) for v in val['val']]
        else:
            raise ValueError(f'invalid type: {t}')
    else:
        return val

def _serialize_none(val):
    return val

def _serialize_bool(val):
    return val

def _serialize_str(val):
    return val

def _serialize_int(val):
    if abs(val) < 2**30:
        return val
    else:
        return {'type': 'int', 'val': hex(val)}

def _serialize_float(val):
    return val

def _serialize_list(val, fmt='any'):
    return {'type': 'list', 'val': [serialize(x, fmt) for x in val]}

def _serialize_tuple(val, fmts=None):
    if fmts is None:
        return {'type': 'tuple', 'val': [serialize(x) for x in val]}
    elif len(val) == len(fmts):
        return {'type': 'tuple', 'val': [serialize(x, f) for (x, f) in zip(val, fmts)]}
    else:
        raise ValueError(f'expecting a tuple of length {len(fmts)}')

def _serialize_set(val, fmt='any'):
    return {'type': 'set', 'val': [serialize(x, fmt) for x in val]}

def _serialize_dict(val, kfmt='any', vfmt='any'):
    return {'type': 'dict', 'val': [(serialize(k, kfmt), serialize(v, vfmt)) for (k,v) in val.items()]}

def _serialize_bytes(val):
    return {'type': 'bytes', 'val': base64.b64encode(val).decode('ascii')}

def _serialize_ref(val):
    return {'type': 'ref', 'val': save_ref(val)}

def _serialize_buffer(val):
    m = memoryview(val)
    assert m.ndim == len(m.shape)
    data = m.tobytes(order='F')
    assert m.nbytes == len(data)
    return {
        'type': 'buffer',
        'format': m.format,
        'itemsize': m.itemsize,
        'ndim': m.ndim,
        'shape': m.shape,
        'data': base64.b64encode(data).decode('ascii'),
    }

def _serialize_array_ndarray(val, eltype, ndim):
    import numpy.lib.format
    arr = numpy.asarray(val)
    if ndim is not None and ndim != arr.ndim:
        return
    data = arr.tobytes(order='F')
    dtype = arr.dtype
    if dtype.hasobject:
        return
    assert arr.ndim == len(arr.shape)
    return {
        'type': 'ndarray',
        'dtype': numpy.lib.format.dtype_to_descr(arr.dtype),
        'ndim': arr.ndim,
        'shape': arr.shape,
        'data': base64.b64encode(data).decode('ascii'),
    }

def _serialize_array_buffer(val, eltype, ndim):
    ans = _serialize_buffer(val)
    if ndim is not None and ndim != len(val['shape']):
        return
    if ans['format'] not in {'b', 'B', 'h', 'H', 'i', 'I', 'l', 'L', 'q', 'Q', 'e', 'f', 'd', 'Ze', 'Zf', 'Zd', '?', 'P'}:
        return
    return ans

def _serialize_array_list(val, eltype, ndim):
    if ndim is None or ndim == 1:
        return _serialize_list(val, eltype)

_SERIALIZE_ARRAY_HANDLERS = [
    _serialize_array_ndarray,
    _serialize_array_buffer,
    _serialize_array_list,
]       

def _serialize_array(val, eltype, ndim):
    for handler in _SERIALIZE_ARRAY_HANDLERS:
        try:
            ans = handler(val, eltype, ndim)
            if ans is not None:
                return ans
        except:
            pass
    raise Exception(f'could not convert to array')

def _serialize_union(val, fmts):
    excs = []
    for (i, fmt) in enumerate(fmts):
        try:
            return {'type': 'union', 'idx': i, 'val': serialize(val, fmt)}
        except BaseException as exc:
            excs.append(exc)
    msg = ' / '.join(str(exc) for exc in excs)
    raise Exception(f'could not convert: {msg}')

def _serialize_media(val, mime):
    for handler in _SERIALIZE_MEDIA_HANDLERS:
        try:
            ans = handler(val, mime)
            if ans is not None:
                return ans
        except:
            pass
    raise Exception(f'could not convert to media type {mime}')

def _serialize_media_mimebundle(val, mime):
    ans = type(val)._repr_mimebundle_(val, include=[mime])
    if isinstance(ans, tuple):
        ans = ans[0]
    ans = ans[mime]
    if isinstance(ans, str):
        ans = ans.encode('utf-8')
    return _serialize_bytes(ans)

REPR_METHODS = {
    "text/plain": "__repr__",
    "text/html": "_repr_html_",
    "text/markdown": "_repr_markdown_",
    "text/json": "_repr_json_",
    "text/latex": "_repr_latex_",
    "application/javascript": "_repr_javascript_",
    "application/pdf": "_repr_pdf_",
    "image/jpeg": "_repr_jpeg_",
    "image/png": "_repr_png_",
    "image/svg+xml": "_repr_svg_",
}

def _serialize_media_repr(val, mime):
    method = REPR_METHODS[mime]
    ans = getattr(type(val), method)(val)
    if isinstance(ans, tuple):
        ans = ans[0]
    if isinstance(ans, str):
        ans = ans.encode('utf-8')
    return _serialize_bytes(ans)

PYPLOT_FORMATS = {
    'image/png': 'png',
    'image/jpeg': 'jpeg',
    'image/tiff': 'tiff',
    'image/svg+xml': 'svg',
    'application/pdf': 'pdf',
}

def _serialize_media_pyplot(val, mime):
    if 'matplotlib' not in sys.modules:
        return
    import matplotlib.pyplot as plt
    fig = val
    while not isinstance(fig, plt.Figure):
        fig = fig.figure
    fmt = PYPLOT_FORMATS[mime]
    buf = io.BytesIO()
    fig.savefig(buf, format=fmt, bbox_inches='tight')
    plt.close(fig)
    return _serialize_bytes(buf.getvalue())

def _serialize_media_bokeh(val, mime):
    if mime != 'text/html':
        return
    if 'bokeh' not in sys.modules:
        return
    from bokeh.models import LayoutDOM
    from bokeh.embed.standalone import autoload_static
    from bokeh.resources import CDN
    if not isinstance(val, LayoutDOM):
        return
    script, html = autoload_static(val, CDN, '')
    # TODO: this is quick hacky
    src = ' src=""'
    endscript = '</script>'
    assert src in html
    assert endscript in html
    html = html.replace(' src=""', '')
    html = html.replace(endscript, script.strip()+endscript)
    return _serialize_bytes(html.encode('utf-8'))

_SERIALIZE_MEDIA_HANDLERS = [
    _serialize_media_pyplot,
    _serialize_media_bokeh,
    _serialize_media_mimebundle,
    _serialize_media_repr,
]

def serialize(val, fmt='any'):
    # parse the format
    if type(fmt) is str:
        args = None
    elif type(fmt) is list:
        hasargs = True
        args = fmt[1:]
        fmt = fmt[0]
    assert type(fmt) is str
    # do the formatting
    if fmt == 'any':
        if val is None:
            return _serialize_none(val)
        elif isinstance(val, bool):
            return _serialize_bool(val)
        elif isinstance(val, str):
            return _serialize_str(val)
        elif isinstance(val, int):
            return _serialize_int(val)
        elif isinstance(val, float):
            return _serialize_float(val)
        elif isinstance(val, list):
            return _serialize_list(val)
        elif isinstance(val, tuple):
            return _serialize_tuple(val)
        elif isinstance(val, dict):
            return _serialize_dict(val)
        elif isinstance(val, set):
            return _serialize_set(val)
        elif isinstance(val, (bytes, bytearray)):
            return _serialize_bytes(val)
        elif isinstance(val, Integral):
            return _serialize_int(int(val))
        try:
            return _serialize_array(val, 'any', None)
        except BaseException:
            pass
        return _serialize_ref(val)
    elif fmt == 'ref':
        return _serialize_ref(val)
    elif fmt == 'none':
        if val is None:
            return _serialize_none(val)
        raise ValueError('expecting None')
    elif fmt == 'bool':
        if isinstance(val, bool):
            return _serialize_bool(val)
        raise ValueError('expecting a bool')
    elif fmt == 'int':
        if isinstance(val, int):
            return _serialize_int(val)
        if isinstance(val, Integral):
            return _serialize_int(int(val))
        raise ValueError('expecting an int')
    elif fmt == 'float':
        if isinstance(val, float):
            return _serialize_float(val)
        elif isinstance(val, int) or isinstance(val, Real):
            return _serialize_float(float(val))
        raise ValueError('expecting a float')
    elif fmt == 'str':
        if isinstance(val, str):
            return _serialize_float(val)
        raise ValueError('expecting a str')
    elif fmt == 'buffer':
        return _serialize_buffer(val)
    elif fmt == 'bytes':
        return _serialize_bytes(bytes(val))
    elif fmt == 'tuple':
        return _serialize_tuple(val, args)
    elif fmt == 'array':
        eltype, ndim = args
        return _serialize_array(val, eltype, ndim)
    elif fmt == 'union':
        return _serialize_union(val, args)
    elif fmt == 'media':
        mime, = args
        return _serialize_media(val, mime)
    raise Exception(f'unexpected format: {fmt}')

class RedirectedStream(io.TextIOBase):
    def __init__(self, tag):
        self.tag = tag
    def write(self, text):
        assert isinstance(text, str), 'input must be a string'
        send({'tag': self.tag, 'text': text})
        return len(text)
    def seekable(self):
        return False
    def readable(self):
        return False
    def writable(self):
        return True

orig_stdout = sys.stdout
orig_stderr = sys.stderr

sys.stdout = RedirectedStream('stdout')
sys.stderr = RedirectedStream('stderr')

while True:
    cmd = recv()
    try:
        tag = cmd['tag']
        if tag == 'echo':
            value = cmd['value']
            send({'tag': 'result', 'value': value})
        elif tag == 'exec':
            code = cmd['code']
            compiled = compile(code, '<input>', 'exec')
            scope = cmd.get('scope')
            if scope is None:
                scope = DEFAULT_MODULE
            mod = sys.modules[scope]
            gs = mod.__dict__
            ls = cmd.get('locals', {})
            if ls:
                ls = {k:deserialize(v) for (k,v) in ls.items()}
            exec(compiled, gs, ls)
            if ls is None:
                ans = None
            else:
                ans = ls.get('ans')
            fmt = cmd.get('format')
            if fmt is None:
                fmt = 'any'
            send({'tag': 'result', 'value': serialize(ans, fmt)})
        elif tag == 'decref':
            for ref in cmd['refs']:
                del_ref(ref)
        elif tag == 'stop':
            context.term()
            time.sleep(1)
            break
        else:
            raise Exception(f'unexpected tag: {tag}')
    except BaseException as exc:
        sys.last_type = type(exc)
        sys.last_value = exc
        sys.last_traceback = exc.__traceback__
        msg = ''.join(traceback.format_exception_only(type(exc), exc))
        tb = ''.join(reversed(traceback.format_list(traceback.extract_tb(exc.__traceback__))))
        send({'tag': 'error', 'msg': msg, 'tb': tb})
