import os, os.path, time, fcntl
import sqlite3, pickle, threading
from collections import deque, namedtuple

class Error(RuntimeError):
	pass

class _Metadata:
	__fmq_metadata = {}
	
	@staticmethod
	def get_metadata(path):
		metadata = _Metadata.__fmq_metadata.get(path)
		if not metadata:
			metadata = _Metadata(path)
			_Metadata.__fmq_metadata[path] = metadata
		return metadata

	class _Lock:
		def __init__(self, file):
			self.__fd = open(file + '.lock', 'w')
			self.__lock = threading.Lock()
		
		def __enter__(self):
			self.__lock.acquire()
			fcntl.lockf(self.__fd, fcntl.LOCK_EX)
		
		def __exit__(self, exc_type, exc_val, exc_tb):
			fcntl.lockf(self.__fd, fcntl.LOCK_UN)
			self.__lock.release()
	
	def __init__(self, path):
		self._meta_file = '%s/Fmq.sdb'%path
		
		self.meta = sqlite3.connect(self._meta_file)
		self.meta.execute('PRAGMA synchronous = OFF')
		self.meta.execute('PRAGMA cache_size = 8000')
		self.meta.execute('PRAGMA case_sensitive_like = 1')
		self.meta.execute('PRAGMA temp_store = MEMORY')
		self.meta.execute('CREATE TABLE IF NOT EXISTS queue_meta(name PRIMARY KEY, partitions, backup_hours)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS queue_logs(log_file PRIMARY KEY, queue, partition, timestamp)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS consume_logs(queue, group_id, partition, log_file, offset)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS consume_registry(queue, group_id, partition, pid)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_q_logs ON queue_logs(queue, timestamp)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_c_logs ON consume_logs(queue, group_id)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_c_reg ON consume_registry(queue, group_id)')
		
		self.lock = _Metadata._Lock(self._meta_file)
	
	def get_queue(self, name):
		c = self.meta.cursor()
		c.execute('SELECT name, partitions, backup_hours FROM queue_meta WHERE name=?', (name,))
		q_meta = c.fetchone()
		if not q_meta:
			return None
		return dict(zip(('name', 'partitions', 'backup_hours'), q_meta))
	
	def create_queue(self, name, **kws):
		q_info = self.get_queue(name)
		if not q_info:
			partitions = kws.get('partitions', 1)
			backup_hours = kws.get('backup_hours', 48)
			assert(partitions>=1 and backup_hours>1)
			c = self.meta.cursor()
			c.execute('INSERT INTO queue_meta(name, partitions, backup_hours) VALUES(?, ?, ?)',
			          (name, partitions, backup_hours))
			self.meta.commit()
			q_info = dict(name=name, partitions=partitions, backup_hours=backup_hours)
		return q_info
	
	def cleanup_expired_logs(self, queue_name, backup_hours):
		c = self.meta.cursor()
		timestamp = (int(time.time())//3600 - backup_hours)*3600
		rows = c.execute('SELECT log_file FROM queue_logs WHERE queue=? AND timestamp<?', (queue_name, timestamp))
		expired_logs = [log_file for (log_file,) in rows]
		c.execute('DELETE FROM queue_logs WHERE queue=? AND timestamp<?', (queue_name, timestamp))
		self.meta.commit()
		return expired_logs
	
	def regist_consumer(self, group_id, queue_name, partition):
		c = self.meta.cursor()
		where_cond = ' WHERE queue=? AND group_id=? AND partition=?'
		c.execute('SELECT pid FROM consume_registry' + where_cond, (queue_name, group_id, partition))
		row = c.fetchone()
		if row:
			(pid,) = row
			try:
				os.kill(pid, 0)
				return False
			except ProcessLookupError:
				c.execute('UPDATE consume_registry SET pid=?' + where_cond,
				          (os.getpid(), queue_name, group_id, partition))
		else:
			c.execute('INSERT INTO consume_registry(queue, group_id, partition, pid) VALUES(?, ?, ?, ?)',
			          (queue_name, group_id, partition, os.getpid()))
		self.meta.commit()
		return True
	
	def unregist_consumer(self, group_id, queue_name, partition):
		c = self.meta.cursor()
		c.execute('DELETE FROM consume_registry WHERE queue=? AND group_id=? AND partition=? AND pid=?',
		          (queue_name, group_id, partition, os.getpid()))
		self.meta.commit()		
	
	def get_logs(self, queue_name, partition, timestamp=None, rows=5):
		c = self.meta.cursor()
		c.execute('SELECT log_file, timestamp FROM queue_logs'
		          ' WHERE queue=? AND partition=? AND timestamp>=?'
		          ' ORDER BY timestamp', (queue_name, partition, timestamp or 0))
		logs = deque()
		for _ in range(rows):
			row = c.fetchone()
			if not row: break
			logs.append(row)
		return logs
	
	def put_log(self, log_file, queue_name, partition, timestamp):
		c = self.meta.cursor()
		try:
			c.execute('INSERT INTO queue_logs(log_file, queue, partition, timestamp) VALUES(?,?,?,?)',
				      (log_file, queue_name, partition, int(timestamp)))
		except sqlite3.IntegrityError:
			pass
	
	def get_consume_log(self, group_id, queue_name, partition):
		c = self.meta.cursor()
		c.execute('SELECT c.log_file, c.offset, q.timestamp '
		          '  FROM consume_logs c, queue_logs q'
		          ' WHERE c.group_id=? AND c.queue=? AND c.partition=?'
		          '   AND q.log_file=c.log_file', (group_id, queue_name, partition))
		return c.fetchone()
	
	def put_consume_log(self, group_id, queue_name, partition, log_file, offset):
		c = self.meta.cursor()
		c.execute('UPDATE consume_logs SET log_file=?, offset=?'
		          ' WHERE group_id=? AND queue=? AND partition=?',
		          (log_file, offset, group_id, queue_name, partition))
		if not c.rowcount:
			c.execute('INSERT INTO consume_logs(group_id, queue, partition, log_file, offset) VALUES(?,?,?,?,?)',
			          (group_id, queue_name, partition, log_file, offset))
	
	def commit(self):
		self.meta.commit()


class Producer:
	def __init__(self, queue_name, path='.', **kws):
		self.__metadata = _Metadata.get_metadata(path)
		with self.__metadata.lock:
			self.queue = self.__metadata.create_queue(queue_name, **kws)
		partitions = self.queue['partitions']
		self.messages = [[] for _ in range(partitions)]
		self.total_sends = [0]*partitions
		self.log_file = [dict(fd=None, name='', timestamp=0) for _ in range(partitions)]
		self.path = '%s/%s'%(path, queue_name)
		if not os.path.exists(self.path):
			os.mkdir(self.path)
	
	def send(self, message, partition=None, key=None):
		partitions = self.queue['partitions']
		if partitions == 1:
			partition = 0
		elif partition is not None:
			assert(0 <= partition < partitions)
		elif key is not None:
			partition = hash(key)%partitions
		else:
			partition = min(zip(self.total_sends, range(partitions)))[1]
		self.messages[partition].append((time.time(), key, message))
		self.total_sends[partition] += 1
	
	def commit(self):
		if not sum(map(len, self.messages)): return
		timestamp = int(time.time())//300*300
		
		partitions = []
		for partition in range(self.queue['partitions']):
			if not self.messages[partition]: continue
			if self.__flush(partition, timestamp):
				partitions.append(partition)
		if not partitions:
			return
		
		with self.__metadata.lock:
			for partition in partitions:
				log_file = self.log_file[partition]
				self.__metadata.put_log(log_file['name'], self.queue['name'], partition, timestamp)
			self.__metadata.commit()
		
		self.cleanup_expired_logs()

	def __flush(self, partition, timestamp):
		log_file, newfile = self.log_file[partition], False
		if timestamp != log_file['timestamp']:
			if log_file['fd']:
				log_file['fd'].close()
				log_file['fd'] = None
			log_file['name'] = '%s.p%d.qdat'%(time.strftime('%Y%m%d%H%M', time.localtime(timestamp)), partition)
			log_file['fd'] = open('%s/%s'%(self.path, log_file['name']), 'ab')
			log_file['timestamp'] = timestamp
			newfile = True
		
		fd = log_file['fd']
		fcntl.lockf(fd, fcntl.LOCK_EX)
		for message in self.messages[partition]:
			pickle.dump(message, fd)
		fd.flush()
		fcntl.lockf(fd, fcntl.LOCK_UN)
		self.messages[partition].clear()
		return newfile
	
	def cleanup_expired_logs(self):
		with self.__metadata.lock:
			expired_logs = self.__metadata.cleanup_expired_logs(self.queue['name'], self.queue['backup_hours'])
		for name in expired_logs:
			log_file = '%s/%s'%(self.path, name)
			os.remove(log_file)


class Consumer:
	Message = namedtuple('ConsumeMessage', ['queue', 'partition', 'key', 'payload', 'timestamp'])
	
	def __init__(self, queue_name, group_id, partition=0, path='.', **kws):
		self.__metadata = _Metadata.get_metadata(path)
		self.queue = self.__metadata.get_queue(queue_name)
		if not self.queue:
			raise Error("Queue '%s' not found"%queue_name)
		with self.__metadata.lock:
			if not self.__metadata.regist_consumer(group_id, queue_name, partition):
				consume_tag = "Cosumer(group_id='{}', queue='{}', partition={})".format(group_id, queue_name, partition)
				raise Error("%s already registry by other consumer"%consume_tag)

		self.partition = partition
		self.group_id = str(group_id)
		self.path = '%s/%s'%(path, queue_name)
		self.auto_ack = kws.get('auto_ack', True)
		
		self.log_file = dict(fd=None, name='', offset=0, timestamp=0)
		self._filelist = deque()
		consume_log = self.__metadata.get_consume_log(self.group_id, self.queue['name'], self.partition)
		if consume_log:
			(self.log_file['name'], self.log_file['offset'], self.log_file['timestamp']) = consume_log
		self.ack_log = None
	
	def __del__(self):
		if hasattr(self, 'partition'):
			with self.__metadata.lock:
				self.__metadata.unregist_consumer(self.group_id, self.queue['name'], self.partition)
	
	def __iter__(self):
		return self
	
	def __next__(self):
		message = self.poll()
		if message is None:
			raise StopIteration
		return message
	
	def poll(self):
		if not self.log_file['fd']:
			self.log_file['fd'] = self.__open_nextfile()
			if not self.log_file['fd']:
				return None
		
		try:
			timestamp, key, message = pickle.load(self.log_file['fd'])
			self.log_file['offset'] = self.log_file['fd'].tell()
			if self.auto_ack: self._ack()
			return Consumer.Message(queue=self.queue['name'], partition=self.partition,
			                        key=key, payload=message, timestamp=timestamp)
		except EOFError:
			pass
		
		self.log_file['fd'].close()
		self.log_file['fd'] = None
		return self.poll()
	
	def commit(self):
		if self.ack_log:
			with self.__metadata.lock:
				self.__metadata.put_consume_log(self.group_id, self.queue['name'], self.partition, **self.ack_log)
				self.__metadata.commit()
	
	def _ack(self):
		self.ack_log = dict(log_file=self.log_file['name'], offset=self.log_file['offset'])
	
	def __open_nextfile(self):
		if self._filelist:
			filename, timestamp = self._filelist.popleft()
		else:
			self._filelist = self.__metadata.get_logs(self.queue['name'], self.partition, self.log_file['timestamp'])
			if not self._filelist:
				return None
			filename, timestamp = self._filelist.popleft()
			
			if timestamp == self.log_file['timestamp']:
				path_name = '%s/%s'%(self.path, filename)
				if self.log_file['offset'] < os.stat(path_name).st_size:
					f = open(path_name, 'rb')
					f.seek(self.log_file['offset'])
					return f
				if not self._filelist:
					return None
				filename, timestamp = self._filelist.popleft()
		
		f = open('%s/%s'%(self.path, filename), 'rb')
		self.log_file['name'], self.log_file['offset'], self.log_file['timestamp'] = filename, 0, timestamp
		return f
				

class MultipleConsumer:
	def __init__(self, queue_name, group_id, path='.', partitions=None, **kws):
		self.__metadata = _Metadata.get_metadata(path)
		self.queue = self.__metadata.get_queue(queue_name)
		if not self.queue:
			raise Error("Queue '%s' not found"%queue_name)
		
		n = self.queue['partitions']
		if partitions is None:
			partitions = set(range(n))
		elif not partitions.issubset(range(n)):
			raise Error("Out of range of partitions")
		
		self.poll_timeout = kws.get('poll_timeout', 0.1)
		self._messages = {}
		self._pconsumers = {}
		for partition in partitions:
			self._messages[partition] = (None, 0)
			self._pconsumers[partition] = \
				Consumer(queue_name, group_id, partition, path, auto_ack=False)
	
	def __del__(self):
		for partition in list(self._pconsumers.keys()):
			del self._pconsumers[partition]
	
	def __iter__(self):
		return self
	
	def __next__(self):
		message = self.poll()
		if message is None:
			raise StopIteration
		return message
	
	def poll(self):
		fetched = []
		for partition, (message, time_nodata) in self._messages.items():
			if not message:
				now = time.time()
				if now - time_nodata > self.poll_timeout:
					message = self._pconsumers[partition].poll()
					if not message:
						self._messages[partition] = (None, now)
			if message:
				self._messages[partition] = (message, 0)
				fetched.append((message.timestamp, partition))
		
		if not fetched: return None
		partition = min(fetched)[1]
		message = self._messages[partition][0]
		self._pconsumers[partition]._ack()
		self._messages[partition] = (None, 0)
		return message
	
	def commit(self):
		for pconsumer in self._pconsumers.values():
			pconsumer.commit()


#########################################################################################################

if __name__ == "__main__":
	path, queue_name, group_id = './fmq', 'test', 'test-group'
	n = 30
	if not os.path.exists(path):
		os.mkdir(path)
	
	p = Producer(queue_name, path=path, partitions=3)
	for i in range(n):
		p.send('This is message %d from process %d'%(i, os.getpid()))
	p.commit()
	
	c1 = Consumer(queue_name, path=path, group_id=group_id, partition=1)
	for m in c1:
		print(m)
	c1.commit()
	
	c02 = MultipleConsumer(queue_name, path=path, group_id=group_id, partitions={0,2})
	for m in c02:
		print(m)
	c02.commit()
	
