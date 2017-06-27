import os, os.path, time, glob, fcntl, re, pickle, threading
from datetime import datetime

class _ObjectPersistentInText:
	@staticmethod
	def dump(data, file):
		file.write(repr(data))
		file.write('\n')
	
	@staticmethod
	def load(file):
		data = file.readline()
		while data.isspace():
			data = file.readline()
		if not data:
			raise EOFError('EOF read')
		return eval(data)

class MessageQueue:
	@staticmethod
	def _filetime(minutes):
		if minutes > 1:
			timestamp = time.time()//(minutes*60)*(minutes*60)
			return time.strftime('%Y%m%d%H%M', time.localtime(timestamp))
		else:
			return time.strftime('%Y%m%d%H%M', time.localtime())
	
	def __init__(self, queue_name, path='.', **kws):
		self.path = path
		self.queue = queue_name
		self._args = kws
	
	def publisher(self):
		return Publisher(self.queue, self.path, **self._args)
	
	def consumer(self, savefile=None, clean_file=None):
		return Consumer(self.queue, self.path, savefile, clean_file, **self._args)

class Publisher:
	def __init__(self, queue_name, path='.', **kws):
		self.path = path + '/' + queue_name
		self.interval = int(kws.get('interval', 1))
		assert(self.interval > 0)
		if self.interval > 1:
			self.path += '-%d'%self.interval
		if not os.path.exists(self.path):
			os.mkdir(self.path)
		self.file_time = None
		self.file = None
		self.messages = []
	
		self._text = kws.get('text', False)
		self._ext = kws.get('ext', 'pytxt' if self._text else 'pybin')
		self._store = kws.get('storage')
		if not self._store:
			self._store = _ObjectPersistentInText if self._text else pickle
		else:
			assert(callable(getattr(self._store, 'load')) and callable(getattr(self._store, 'dump')))
	
	def publish(self, message):
		self.messages.append(message)
	
	def rollback(self):
		self.messages.clear()
	
	def commit(self):
		if not self.messages: return
		file_time = MessageQueue._filetime(self.interval)
		if file_time != self.file_time:
			if self.file:
				self.file.close()
				self.file = None
			filename = '%s/%s.%s'%(self.path, file_time, self._ext)
			self.file = open(filename, 'a' if self._text else 'ab')
			self.file_time = file_time
		
		fcntl.lockf(self.file, fcntl.LOCK_EX)
		for message in self.messages:
			self._store.dump(message, self.file)
		self.file.flush()
		fcntl.lockf(self.file, fcntl.LOCK_UN)
		self.messages.clear()

class Consumer:
	__mutex = threading.Lock()

	def __init__(self, queue_name, path='.', savefile=None, clean_file=None, **kws):
		self.path = path + '/' + queue_name
		self.interval = int(kws.get('interval', 1))
		assert(self.interval > 0)
		if self.interval > 1:
			self.path += '-%d'%self.interval
		
		self._parallel = kws.get('parallel', (1, 0))
		assert(isinstance(self._parallel, tuple) and 0 <= self._parallel[1] < self._parallel[0])
		if self._parallel[0] == 1:
			self.savefile = savefile or '%s/%s.sav'%(self.path, queue_name)
		else:
			self.savefile = savefile or '%s/%s.%d-%d.sav'%(self.path, queue_name, self._parallel[0], self._parallel[1])
		self._lockfile = open(self.savefile + '.pid', 'w')
		fcntl.lockf(self._lockfile, fcntl.LOCK_EX|fcntl.LOCK_NB)
		self._lockfile.write(str(os.getpid()))
		self._lockfile.flush()
		
		self._text = kws.get('text', False)
		self._ext = kws.get('ext', 'pytxt' if self._text else 'pybin')
		self._store = kws.get('storage')
		if not self._store:
			self._store = _ObjectPersistentInText if self._text else pickle
		else:
			assert(callable(getattr(self._store, 'load')) and callable(getattr(self._store, 'dump')))
		self.__filename_formatter = re.compile(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})\.' + self._ext + r'$')
		
		self.clean_file = clean_file
		if clean_file: self._clean_files = set()
		self._filelist = []
		self._recovery()
	
	def _recovery(self):
		self.file, self.file_name, self.file_pos = None, '', 0
		try:
			with open(self.savefile, 'r') as f:
				self.file_name, self.file_pos = eval(f.read())
				if self.file_name:
					self.file = open(self.path + '/' + self.file_name, 'r' if self._text else 'rb')
					self.file.seek(self.file_pos)
		except:
			 pass
	
	def __iter__(self):
		return self
	
	def __next__(self):
		message = self.consume()
		if message is None:
			raise StopIteration
		return message
	
	def consume(self):
		if not self.file:
			self.file = self.__open_nextfile()
			if not self.file: return None
		
		try:
			message = self._store.load(self.file)
			self.file_pos = self.file.tell()
			return message
		except EOFError:
			pass
		
		self.file.close()
		self.file = None
		return self.consume()
	
	def rollback(self):
		self._recovery()
		if self.clean_file:
			self._clean_files.clear()
	
	def commit(self):
		with open(self.savefile, 'w') as f:
			f.write(repr((self.file_name, self.file_pos)))
		if callable(self.clean_file):
			for file_name in self._clean_files:
				try:
					self.clean_file(self.path, file_name)
				except:
					pass
			self._clean_files.clear()
	
	def __filter_filename(self, filename):
		try:
			matched = self.__filename_formatter.match(filename)
			if not matched: return False
			if self._parallel[0] == 1: return True
			filetime = int(datetime(*map(int, matched.groups())).timestamp())
			return ((filetime//(60*self.interval)) % self._parallel[0] == self._parallel[1])
		except:
			return False
	
	def __open_nextfile(self):
		if self._filelist:
			file_name = self._filelist.pop()
		else:
			with self.__mutex:
				pwd = os.getcwd()
				os.chdir(self.path)
				self._filelist = [filename for filename in glob.iglob('20*.'+self._ext) \
				                  if filename >= self.file_name and self.__filter_filename(filename)]
				os.chdir(pwd)
			if not self._filelist: return None
			self._filelist.sort(reverse=True)
			file_name = self._filelist.pop()
			
			if file_name == self.file_name:
				path_name = self.path + '/' + file_name
				if self.file_pos < os.stat(path_name).st_size:
					f = open(path_name, 'r' if self._text else 'rb')
					f.seek(self.file_pos)
					return f
				if not self._filelist:
					if self.clean_file:
						now_filetime = MessageQueue._filetime(self.interval)
						if self.file_name < '%s.%s'%(now_filetime, self._ext):
							self._clean_files.add(self.file_name)
					return None
				file_name = self._filelist.pop()
		
		if self.clean_file and self.file_name:
			self._clean_files.add(self.file_name)
		f = open(self.path + '/' + file_name, 'r' if self._text else 'rb')
		self.file_name, self.file_pos = file_name, 0
		return f
