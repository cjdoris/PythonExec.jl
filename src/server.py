import base64
import json
import io
import sys
import time
import traceback

stdin = sys.stdin
stdout = sys.stdout
stderr = sys.stderr

DEFAULT_MODULE = '__main__.userspace'

userspace = sys.modules[DEFAULT_MODULE] = type(sys)(DEFAULT_MODULE)

def send(msg):
    json.dump(msg, stdout)
    stdout.write('\n')
    stdout.flush()

def recv():
    line = stdin.readline().strip()
    if not line:
        raise Exception('unexpected end of input')
    return json.loads(line)

def send_result(value, format='any'):
    send({'tag': 'result', 'value': format_value(value, format)})

def format_value(val, fmt):
    if type(fmt) is str:
        if fmt == 'any':
            if val is None or isinstance(val, (float, bool, str)):
                return val
            elif isinstance(val, int):
                if abs(val) < 2**30:
                    return val
                else:
                    return {'type': 'int', 'val': hex(val)}
            elif isinstance(val, list):
                return {'type': 'list', 'val': [format_value(x, fmt) for x in val]}
            elif isinstance(val, tuple):
                return {'type': 'tuple', 'val': [format_value(x, fmt) for x in val]}
            elif isinstance(val, dict):
                return {'type': 'dict', 'val': [[format_value(x, fmt) for x in item] for item in val.items()]}
            elif isinstance(val, (set, frozenset)):
                return {'type': 'set', 'val': [format_value(x, fmt) for x in val]}
            elif isinstance(val, (bytes, bytearray)):
                return {'type': 'bytes', 'val': base64.b64encode(val).decode('ascii')}
            else:
                raise Exception(f'unexpected type: {type(val).__name__}')
        elif fmt == 'none':
            if val is None:
                return None
            raise ValueError('expecting None')
        elif fmt == 'bool':
            if val is True or val is False:
                return val
            raise ValueError('expecting a bool')
        elif fmt == 'int':
            if isinstance(val, int):
                if abs(val) < 2**30:
                    return int(val)
                else:
                    return hex(val)
            raise ValueError('expecting an int')
        elif fmt == 'float':
            if isinstance(val, (float, int)):
                return float(val)
            raise ValueError('expecting a float')
        elif fmt == 'str':
            if isinstance(val, str):
                return str(val)
            raise ValueError('expecting a str')
        elif fmt == 'buffer':
            m = memoryview(val)
            return {
                'format': m.format,
                'itemsize': m.itemsize,
                'nbytes': m.nbytes,
                'ndim': m.ndim,
                'shape': m.shape,
                'data': base64.b64encode(m.tobytes(order='F')).decode('ascii'),
            }
        elif fmt == 'bytes':
            x = bytes(val)
            return base64.b64encode(x).decode('ascii')
        elif fmt == 'tuple':
            return [format_value(x, 'any') for x in val]
    elif type(fmt) is list:
        args = fmt[1:]
        fmt = fmt[0]
        if fmt == 'array':
            eltype, ndim = args
            if ndim is None or ndim == 1:
                return [format_value(x, eltype) for x in val]
            elif ndim > 0:
                return [format_value(x, ['array', eltype, ndim-1]) for x in val]
            else:
                raise Exception(f'cannot serialize iterator to zero dimensions')
        elif fmt == 'union':
            excs = []
            for (i, fmt2) in enumerate(args):
                try:
                    return {'idx': i, 'val': format_value(val, fmt2)}
                except BaseException as exc:
                    excs.append(exc)
            msg = ' / '.join(str(exc) for exc in excs)
            raise Exception(f'could not convert: {msg}')
        elif fmt == 'tuple':
            return [format_value(x, t) for (x, t) in zip(val, args)]
        elif fmt == 'media':
            mime = args[0]
            assert mime == 'image/png'
            buf = io.BytesIO()
            val.savefig(buf, format='png', bbox_inches='tight')
            return format_value(buf.getvalue(), 'bytes')
    raise Exception(f'unexpected format: {fmt}')

while True:
    cmd = recv()
    try:
        tag = cmd['tag']
        if tag == 'echo':
            msg = cmd['msg']
            send_result(msg)
        elif tag == 'exec':
            code = cmd['code']
            compiled = compile(code, '<input>', 'exec')
            scope = cmd.get('scope')
            if scope is None:
                scope = DEFAULT_MODULE
            mod = sys.modules[scope]
            gs = mod.__dict__
            ls = cmd.get('locals', {})
            exec(compiled, gs, ls)
            if ls is None:
                ans = None
            else:
                ans = ls.get('ans')
            fmt = cmd.get('format')
            if fmt is None:
                fmt = 'any'
            send_result(ans, fmt)
        elif tag == 'stop':
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
