import capi, ctypes, errno
try:
	from ipchdr import *
except ImportError:
	raise ImportError("Please compile 'dump_ipchdr.c' and run")

ftok = capi.cfunc('ftok', 'int', ('path', 'char*'), ('id', 'int'))
getpagesize = capi.cfunc('getpagesize', 'int')

IPC_PRIVATE = ipc_define['IPC_PRIVATE']

IPC_CREAT   = ipc_define['IPC_CREAT']
IPC_EXCL    = ipc_define['IPC_EXCL']
IPC_CREX    = IPC_CREAT | IPC_EXCL

IPC_NOWAIT  = ipc_define['IPC_NOWAIT']

SHM_RDONLY  = ipc_define['SHM_RDONLY']
SEM_UNDO    = ipc_define['SEM_UNDO']

IPC_RMID    = ipc_define['IPC_RMID']
IPC_STAT    = ipc_define['IPC_STAT']
GETVAL      = ipc_define['GETVAL']
GETALL      = ipc_define['GETALL']
SETVAL      = ipc_define['SETVAL']
SETALL      = ipc_define['SETALL']

def _make_fields(fields, end, start=0):
	result, seq = [], 1
	_types = { 1: ctypes.c_uint8, 2: ctypes.c_uint16, 4:ctypes.c_uint32, 8:ctypes.c_uint64 }
	for name, pos, n in sorted(fields, key=lambda kv: kv[1]):
		pad = pos - start
		if pad:
			result.append(('__pad%d'%seq, ctypes.c_ubyte * pad))
			seq += 1
		result.append((name, _types[n]))
		start = pos + n
	if start < end:
		result.append(('__pad%d'%seq, ctypes.c_ubyte * (end - start)))
	return result

class ipc_perm(ctypes.Structure):
	_fields_ = _make_fields(ipc_struct['ipc_perm']['fields'], ipc_struct['ipc_perm']['size'])

class msqid_ds(ctypes.Structure):
	_fields_ = [('msg_perm', ipc_perm)] + \
		_make_fields(ipc_struct['msqid_ds']['fields'], \
		             ipc_struct['msqid_ds']['size'], \
		             ipc_struct['ipc_perm']['size'])

class shmid_ds(ctypes.Structure):
	_fields_ = [('shm_perm', ipc_perm)] + \
		_make_fields(ipc_struct['shmid_ds']['fields'], \
		             ipc_struct['shmid_ds']['size'], \
		             ipc_struct['ipc_perm']['size'])

class semid_ds(ctypes.Structure):
	_fields_ = [('sem_perm', ipc_perm)] + \
		_make_fields(ipc_struct['semid_ds']['fields'], \
		             ipc_struct['semid_ds']['size'], \
		             ipc_struct['ipc_perm']['size'])

############################################### Message Queue ###############################################

msgget = capi.cfunc('msgget', 'int', \
         ('key', 'int'), ('msgflg', 'int'))
msgsnd = capi.cfunc('msgsnd', 'int', \
         ('msqid', 'int'), ('msgp', 'void*'), ('msgsz', 'size_t'), ('msgflg', 'int'))
msgrcv = capi.cfunc('msgrcv', 'ssize_t', \
         ('msqid', 'int'), ('msgp', 'void*'), ('msgsz', 'size_t'), ('msgtyp', 'long'), ('msgflg', 'int'))
msgctl = capi.cfunc('msgctl', 'int', \
         ('msqid', 'int'), ('cmd', 'int'), ('msqid_ds', 'void*', None))

def remove_message_queue(id):
	msgctl(id, IPC_RMID)

class MessageQueue:
	def __init__(self, key, flags=0, mode=0o600, max_message_size=2048, **kws):
		if kws.get('not_key') is True:
			self.__id = key
			self.__stbuf = msqid_ds()
			self.__key = self.stat.msg_perm.key
		else:
			self.__id = msgget(ctypes.c_int(key), flags|mode)
			self.__key = key
			self.__stbuf = msqid_ds()
		
		class msgbuf(ctypes.Structure):
			_fields_ = [('type', ctypes.c_long), ('mtext', ctypes.c_ubyte * max_message_size)]
		self.__msgbuf = msgbuf()
	
	@property
	def id(self):
		return self.__id
	
	@property
	def key(self):
		return self.__key
	
	def __len__(self):
		return self.stat.msg_qnum
	
	def send(self, message, block=True, type=1):
		if isinstance(message, str) and bytes is not str:
			message = message.encode()
		elif not isinstance(message, bytes):
			raise TypeError('message must be bytes')
		if len(message) > len(self.__msgbuf.mtext):
			raise OSError(errno.E2BIG, 'The message text length is greater than max_message_size')
		self.__msgbuf.type = type
		ctypes.memmove(self.__msgbuf.mtext, message, len(message))
		msgsnd(self.id, ctypes.byref(self.__msgbuf), len(message), 0 if block else IPC_NOWAIT)
	
	def receive(self, block=True, type=0):
		try:
			msglen = msgrcv(self.id, ctypes.pointer(self.__msgbuf), len(self.__msgbuf.mtext), type, 0 if block else IPC_NOWAIT)
			message = b'\0' * msglen
			ctypes.memmove(message, self.__msgbuf.mtext, msglen)
			return message, self.__msgbuf.type
		except OSError as e:
			if e.errno == errno.ENOMSG:
				return None
			raise
	
	def remove(self):
		msgctl(self.id, IPC_RMID)

	@property
	def stat(self):
		msgctl(self.id, IPC_STAT, ctypes.pointer(self.__stbuf))
		return self.__stbuf

############################################### Shared Memory ###############################################

shmget = capi.cfunc('shmget', 'int', ('key', 'int'), ('size', 'size_t'), ('shmflg', 'int'))
shmctl = capi.cfunc('shmctl', 'int', ('shmid', 'int'), ('cmd', 'int'), ('shmid_ds', 'void*', None))
shmat  = capi.cfunc('shmat', 'void*',  ('shmid', 'int'), ('shmaddr', 'void*', None), ('shmflg', 'int', 0))
shmdt  = capi.cfunc('shmdt', 'int', ('shmaddr', 'void*'))

def attach(id, readonly=False):
	shm = SharedMemory(id, not_key=True)
	shm.attach(readonly)
	return shm

def remove_shared_memory(id):
	shmctl(id, IPC_RMID)

class SharedMemory:
	def __init__(self, key, size=0, flags=0, mode=0o600, **kws):
		if kws.get('not_key') is True:
			self.__id = key
			self.__stbuf = shmid_ds()
			_stat = self.stat
			self.__key, self.__size = _stat.shm_perm.key, _stat.shm_segsz
		else:
			if flags & IPC_CREAT:
				if size == 0: size = getpagesize()
			self.__id = shmget(ctypes.c_int(key), size, flags|mode)
			self.__key = key
			self.__stbuf = shmid_ds()
			self.__size = self.stat.shm_segsz
		self.__address = None
	
	@property
	def id(self):
		return self.__id
	
	@property
	def key(self):
		return self.__key
	
	def __len__(self):
		return self.__size
	
	def attach(self, readonly=False):
		if self.__address is None:
			self.__address = shmat(self.id, None, SHM_RDONLY if readonly else 0)
	
	def detach(self):
		if self.__address is not None:
			shmdt(self.__address)
			self.__address = None
	
	def read(self, byte_count, offset=0):
		if not (0 <= offset < self.__size):
			raise IndexError('offset out of range')
		ptr = ctypes.cast(self.__address + offset, ctypes.POINTER(ctypes.c_ubyte))
		n = min(byte_count, self.__size - offset)
		buf = b'\0' * n
		ctypes.memmove(buf, ptr, n)
		return buf
	
	def write(self, some_bytes, offset=0):
		if not isinstance(some_bytes, bytes):
			raise TypeError('write buffer must be bytes')
		if not (0 <= offset <= self.__size - len(some_bytes)):
			raise IndexError('offset should out of range')
		ptr = ctypes.cast(self.__address + offset, ctypes.POINTER(ctypes.c_ubyte))
		ctypes.memmove(ptr, some_bytes, len(some_bytes))
	
	def remove(self):
		shmctl(self.id, IPC_RMID)
	
	@property
	def stat(self):
		shmctl(self.id, IPC_STAT, ctypes.pointer(self.__stbuf))
		return self.__stbuf
	
	def __getitem__(self, index):
		if isinstance(index, int):
			if index < 0: index += self.__size
			if not (0 <= index < self.__size):
				raise IndexError('index out of range')
			return self.read(1, index)
		elif isinstance(index, slice):
			start, stop, step = index.start or 0, index.stop or self.__size, index.step
			assert(step is None or step==1)
			if start < 0: start += self.__size
			if stop < 0: stop += self.__size
			return self.read(stop - start, start)
		else:
			raise TypeError('indices must be integer or slice')

################################################# Semaphore #################################################

class sembuf(ctypes.Structure):
	_fields_ = [('sem_num', ctypes.c_ushort), ('sem_op', ctypes.c_short), ('sem_flg', ctypes.c_short)]

semget = capi.cfunc('semget', 'int', \
         ('key', 'int'), ('nsems', 'int'), ('semflg', 'int'))
semop  = capi.cfunc('semop', 'int', \
         ('semid', 'int'), ('sops', ctypes.POINTER(sembuf)), ('nsops', 'u_int'))
semctl = capi.cfunc('semctl', 'int', \
         ('semid', 'int'), ('semnum', 'int'), ('cmd', 'int'), ('semid_ds', 'void*', None))

def remove_semaphore(id):
	semctl(id, 0, IPC_RMID)

class BaseSemaphore:
	def __init__(self, key, nsems=1, flags=0, mode=0o600, initial_value=0, **kws):
		self.__values = None
		if kws.get('not_key') is True:
			self.__id = key
			self.__stbuf = semid_ds()
			_stat = self.stat
			self.__key, self.__n = _stat.sem_perm.key, _stat.sem_nsems
		else:
			if flags & IPC_CREAT:
				try:
					self.__id = semget(ctypes.c_int(key), nsems, IPC_EXCL|flags|mode)
					
					values = [initial_value] * nsems
					self.__values = (ctypes.c_ushort * nsems)(*values)
					semctl(self.id, 0, SETALL, self.__values)
				
				except FileExistsError:
					if flags & IPC_EXCL: raise
					self.__id = semget(ctypes.c_int(key), nsems, flags|mode)
			else:
				self.__id = semget(ctypes.c_int(key), 0, 0)
			self.__key = key
			self.__stbuf = semid_ds()
			self.__n = self.stat.sem_nsems
	
	@property
	def id(self):
		return self.__id
	
	@property
	def key(self):
		return self.__key
	
	def __len__(self):
		return self.__n

	def _op(self, sem_ops):
		n = len(sem_ops)
		sops = (sembuf * n)()
		for i in range(n):
			num, op, flags = sem_ops[i]
			if not (0 <= num < self.__n):
				raise IndexError('sem_num is out of range')
			sops[i].sem_num = num
			sops[i].sem_op = op
			sops[i].sem_flg = flags
		semop(self.id, sops, n)
	
	def value(self, index=None):
		if index is None:
			if self.__values is None:
				self.__values = (ctypes.c_ushort * self.__n)()
			semctl(self.id, 0, GETALL, self.__values)
			return list(self.__values)
		
		if not (0 <= index < self.__n):
			raise IndexError('index out of range')
		return semctl(self.id, index, GETVAL)
	
	def remove(self):
		semctl(self.id, 0, IPC_RMID)
	
	@property
	def stat(self):
		semctl(self.id, 0, IPC_STAT, ctypes.pointer(self.__stbuf))
		return self.__stbuf
	
	def __getitem__(self, index):
		if isinstance(index,int):
			if not (0 <= index < self.__n):
				raise IndexError('index out of range')
			return Semaphore(self.id, not_key=True, index=index)
		else:
			raise TypeError('indices must be integer')

class Semaphore(BaseSemaphore):
	def __init__(self, key, flags=0, mode=0o600, initial_value=0, **kws):
		super().__init__(key, 1, flags, mode, initial_value, **kws)
		index = kws.get('index', 0)
		if not (0 <= index < len(self)): 
			raise IndexError('index out of range')
		self.__index = index
	
	@property
	def value(self):
		return super().value(self.__index)
	
	def _op(self, delta, block=True, undo=True):
		sem_flg = 0
		if delta and undo: sem_flg |= SEM_UNDO
		if not block: sem_flg |= IPC_NOWAIT
		super()._op([(self.__index, delta, sem_flg)])
	
	def acquire(self, delta=1, block=True, undo=True):
		assert(delta > 0)
		self._op(-delta, block, undo)
	
	def release(self, delta=1):
		assert(delta > 0)
		self._op(delta, block=False)
	
	def P(self, block=True):
		self._op(-1, block)
	
	def V(self):
		self._op(1, block=False)
	
	def Z(self, block=True):
		self._op(0, block, undo=False)
