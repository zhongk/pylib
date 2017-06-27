import re

__all__ = ['loads', 'dumps']

def dumps(obj, sort_keys=False, skipkeys=False, indent=None):
	if isinstance(obj, str):
		return encode_string(obj)
	
	if indent and not isinstance(indent, str):
		if isinstance(indent, int) and indent is not True:
			indent = ' '*indent if indent>0 else '\t'
		else:
			indent = '\t'
	
	chunks = []
	encode_object(chunks, obj, skipkeys, sort_keys, indent, 0)
	return ''.join(chunks)

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'\s*', FLAGS)

def loads(s):
	try:
		obj, end = parse_object(s, 0)
	except IndexError:
		raise ValueError('Lua object unclosed')
	end = WHITESPACE.match(s, end).end()
	if end != len(s):
		raise ValueError("Extra data at %d" % end)
	return obj

LUAT_KEY = r'(\[(?P<K>.+?)\]|(?P<N>[_A-Za-z]\w*))'
LUAT_VALUE = r'''(?P<V>{|(?P<quote>['"]).*?[^\\](?P=quote)|[^\s,}]+)'''
LUAT_RE = re.compile(LUAT_KEY + r'\s*=\s*' + LUAT_VALUE, FLAGS)

def simple_loads(t):
	assert(isinstance(t, str))
	def replace(m):
		if m.group('N'):
			return '"%s":%s'%(m.group('N'), m.group('V'))
		else:
			return '%s:%s'%(m.group('K'), m.group('V'))
	s = LUAT_RE.sub(replace, t)
	return eval(s, {'nil':None, 'true':True, 'false':False})

########################################################################################################

ESCAPE = re.compile(r'[\x00-\x1f\\"\a\b\f\n\r\t\v]')
ESCAPE_DCT = {
	'\\': '\\\\', '"': '\\"', '\a': '\\a',
	'\b': '\\b', '\f': '\\f', '\n': '\\n',
	'\r': '\\r', '\t': '\\t', '\v': '\\v',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\%03d' % (i,))

def encode_string(s):
	def replace(match):
		return ESCAPE_DCT[match.group(0)]
	return '"' + ESCAPE.sub(replace, s) + '"'

def encode_object(L, v, skipkeys, sort_keys, indent, indent_level):
	if v is None:
		L.append('nil')
	elif v is True:
		L.append('true')
	elif v is False:
		L.append('false')
	elif isinstance(v, str):
		L.append(encode_string(v))
	elif isinstance(v, (int,float)):
		L.append(str(v))
	elif isinstance(v, dict):
		encode_table(True, L, v, skipkeys, sort_keys, indent, indent_level+1)
	elif isinstance(v, (tuple,list)):
		encode_table(False, L, v, skipkeys, sort_keys, indent, indent_level+1)
	else:
		raise TypeError("unsupport python type '%s'"%type(v).__name__)

def encode_table(dct, L, t, skipkeys, sort_keys, indent, indent_level):
	if not t:
		L.append('{}')
		return
	
	buf = '{'
	if not indent:
		separator = ', '
	else:
		newline_indent = '\n' + indent * indent_level
		separator = ',' + newline_indent
		buf += newline_indent
	
	first = True
	_t = sorted(t.keys(), key=lambda k:(1 if isinstance(k,str) else 0, k)) if dct and sort_keys else t
	for v in _t:
		if first:
			first = False
		else:
			buf = separator
		L.append(buf)
		
		if dct:
			k, v = v, t[v]
			if isinstance(k, str):
				k = encode_string(k)
			elif isinstance(k, (int,float)) and k is not True and k is not False:
				k = str(k)
			elif skipkeys:
				continue
			else:
				raise TypeError("key " + repr(k) + " is not a string")
			L.append('[' + k + ']=')
		
		encode_object(L, v, skipkeys, sort_keys, indent, indent_level)
	
#	L.append(',')
	if indent:
		L.append('\n' + indent * (indent_level-1))
	L.append('}')

########################################################################################################

def parse_object(string, idx):
	nextchar = string[idx]
	if nextchar=='"' or nextchar=="'":
		return parse_string(string, idx+1)
	elif nextchar == '{':
		return parse_table(string, idx+1)
	elif nextchar.isalpha() or nextchar=='_':
		var, end = parse_varname(string, idx)
		if isinstance(var, str):
			raise ValueError("Name '%s' is not defined at %d" % (var,idx-len(var)))
		return var, end
	else:
		return parse_number(string, idx)

NUMBER_RE = re.compile(r'([-+]?0[xX][\dA-Fa-f]+)|([-+]?\d+(\.\d+)?([eE][-+]?\d+)?)', FLAGS)
match_number = NUMBER_RE.match

def parse_number(string, idx):
	m = match_number(string, idx)
	if not m:
		raise ValueError("Expecting value at %d" % idx)
	hexstr, number, frac, exp = m.groups()
	if hexstr is not None:
		return int(hexstr, 16), m.end()
	elif frac or exp:
		return float(number), m.end()
	else:
		return int(number), m.end()

VARNAME_RE = re.compile(r'([a-zA-Z_]\w*)', FLAGS)
match_var = VARNAME_RE.match
CONSTANT = {'nil': None, 'true': True, 'false': False}

def parse_varname(string, idx):
	m = match_var(string, idx)
	if not m:
		raise ValueError("Unexpect character at %d" % idx)
	var = m.group(0)
	return CONSTANT.get(var, var), idx+len(var)

STRINGCHUNK = {
	'"': re.compile(r'(.*?)(["\\])', FLAGS),
	"'": re.compile(r"(.*?)(['\\])", FLAGS)
}
BACKSLASH = {
	'"': '"', "'": "'", '\\': '\\',
	'/': '/', 'a': '\a', 'b': '\b',
	'f': '\f', 'r': '\r', 'n': '\n',
	't': '\t', 'v': '\v',
}

def _decode_escape_decimal(s, pos):
	end = pos + 1
	for i in range(2):
		if not s[end].isnumeric(): break
		end += 1
	esc = s[pos:end]
	
	dec = int(esc)
	if dec > 0xff:
		raise ValueError("Decimal escape too large near '\\%s' at %d" % (esc,pos))
	return dec, end

def _decode_escape_hexadecimal(s, pos):
	end = pos + 2
	esc = s[pos:end]
	try:
		dec = int(esc, 16)
	except:
		raise ValueError("Hexadecimal digit expected near '\\x%s' at %d" % (esc,pos))
	return dec, end

def parse_string(s, end):
	chunks = []
	_append = chunks.append
	begin = end - 1
	_b, _m = BACKSLASH, STRINGCHUNK[s[begin]].match
	while 1:
		chunk = _m(s, end)
		if chunk is None:
			raise ValueError("Unterminated string starting at %d" % begin)
		end = chunk.end()
		content, terminator = chunk.groups()
		if content:
			_append(content)
		if terminator != '\\':
			break
		try:
			esc = s[end]
		except IndexError:
			raise ValueError("Unterminated string starting at %d" % begin)
		if not esc.isnumeric():
			try:
				if esc == 'x':
					asc, end = _decode_escape_hexadecimal(s, end+1)
					char = chr(asc)
				else:
					char = _b[esc]
					end += 1
			except KeyError:
				raise ValueError("Invalid \\escape: {0!r}".format(esc))
		else:
			asc, end = _decode_escape_decimal(s, end)
			char = chr(asc)
		_append(char)
	return ''.join(chunks), end

def parse_table(s, end):
	_w = WHITESPACE.match
	obj, idx = {}, 1
	end = _w(s, end).end()
	nextchar = s[end]
	
	while nextchar != '}':
		key = None
		if nextchar == '[':
			end = _w(s, end+1).end()
			nextchar = s[end]
			
			if nextchar=='"' or nextchar=="'":
				key, end = parse_string(s, end+1)
			else:
				try:
					key, end = parse_number(s, end)
				except:
					raise ValueError("Index is only string or number at %d" % end)
			
			end = _w(s, end).end()
			nextchar = s[end]
			if nextchar != ']':
				raise ValueError("Expecting index enclosed in ']' at %d" % end)
			
			end = _w(s, end+1).end()
			nextchar = s[end]
			if nextchar != '=':
				raise ValueError("Expecting assignment operator '=' at %d" % end)
			
			end = _w(s, end+1).end()
			nextchar = s[end]
		
		elif nextchar.isalpha() or nextchar=='_':
			begin = end
			var, end = parse_varname(s, end)
			
			end = _w(s, end).end()
			nextchar = s[end]
			if nextchar == '=':
				if not isinstance(var, str):
					raise ValueError("Index is only string or number at %d" % begin)
				key = var
				end = _w(s, end+1).end()
				nextchar = s[end]
			else:
				if isinstance(var, str):
					raise ValueError("Name '%s' is not defined at %d" % (var, begin))
				obj[idx], idx = var, idx+1
				
				end = _w(s, end).end()
				nextchar = s[end]
				if nextchar == ',':
					end = _w(s, end+1).end()
					nextchar = s[end]
					continue
		
		value, end = parse_object(s, end)  
		if key is None: key, idx = idx, idx+1
		obj[key] = value
		
		end = _w(s, end).end()
		nextchar = s[end]
		if nextchar == ',':
			end = _w(s, end+1).end()
			nextchar = s[end]
	
	if obj and idx==len(obj)+1:
		obj = [obj[i] for i in range(1, idx)]
	return obj, end+1

########################################################################################################

if __name__ == "__main__":
	t = dict(a=1, b=2.5, c=False, d=dict(e=5, f=6), g=None, l=['a','b',{'x':1,'y':2,'z':3}],
	         s="{a=1, [\"b\"]=0xfe,\t['c']=true,\n['d']={[-0.1]=-10.5e-2, 'a', 'b', c={3,2,1,}, d={}}}")
	s = dumps(t, sort_keys=True)
	print('dumps:', s)
	d = loads(s)
	print('\nloads:', d)
	assert(s == dumps(d, sort_keys=True))
	
	print('\nloads(%s) return :'%repr(d['s']))
	print(loads(d['s']))
