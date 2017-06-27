import ctypes, ctypes.util, os, re

def clib(name):
	return ctypes.CDLL(ctypes.util.find_library(name))

libc = clib('c')

def _errcheck(result, func, args):
	if result == -1:
		errno = ctypes.get_errno()
		raise OSError(errno, os.strerror(errno))
	return result

def cfunc(name, restype, *args, **kws):
	def parse_type(typename):
		tp_name, points = re.match(r'^(.+?)\s*(\**)$', typename).groups()
		pt_level = len(points)
		if pt_level and (tp_name=='char' or tp_name=='wchar_t' or tp_name=='void'):
			tp_name += '*'
			pt_level -= 1
		ret_type = ctype[tp_name]
		for i in range(pt_level):
			ret_type = ctypes.POINTER(ret_type)
		return ret_type
	
	argtypes, paramflags = [], []
	for arg in args:
		argtypes.append(parse_type(arg[1]) if isinstance(arg[1], str) else arg[1])
		paramflags.append((1, arg[0]) + arg[2:])
	
	dll, errcheck = kws.get('dll', libc), kws.get('errcheck', _errcheck)
	if isinstance(restype, str):
		restype = parse_type(restype)
	prototype = ctypes.CFUNCTYPE(restype, *argtypes, use_errno=True)
	func = prototype((name, dll), tuple(paramflags))
	if callable(errcheck):
		func.errcheck = errcheck
	return func

ctype = {
	"bool": ctypes.c_bool,
	"char": ctypes.c_char,
	"wchar_t": ctypes.c_wchar,
	"int8_t": ctypes.c_int8,
	"int16_t": ctypes.c_int16,
	"int32_t": ctypes.c_int32,
	"int64_t": ctypes.c_int64,
	"uint8_t": ctypes.c_uint8,
	"uint16_t": ctypes.c_uint16,
	"uint32_t": ctypes.c_uint32,
	"uint64_t": ctypes.c_uint64,
	"size_t": ctypes.c_size_t,
	"ssize_t": ctypes.c_ssize_t,
	"byte": ctypes.c_byte,
	"short": ctypes.c_short,
	"int": ctypes.c_int,
	"long": ctypes.c_long,
	"long long": ctypes.c_longlong,
	"u_char": ctypes.c_ubyte,
	"u_short": ctypes.c_ushort,
	"u_int": ctypes.c_uint,
	"u_long": ctypes.c_ulong,
	"signed": ctypes.c_int,
	"signed char": ctypes.c_byte,
	"signed short": ctypes.c_short,
	"signed int": ctypes.c_int,
	"signed long": ctypes.c_long,
	"signed long long": ctypes.c_longlong,
	"unsigned": ctypes.c_uint,
	"unsigned char": ctypes.c_ubyte,
	"unsigned short": ctypes.c_ushort,
	"unsigned int": ctypes.c_uint,
	"unsigned long": ctypes.c_ulong,
	"unsigned long long": ctypes.c_ulonglong,
	"float": ctypes.c_float,
	"double": ctypes.c_double,
	"long double": ctypes.c_longdouble,
	"char*": ctypes.c_char_p,
	"wchar_t*": ctypes.c_wchar_p,
	"void*": ctypes.c_void_p,
}
