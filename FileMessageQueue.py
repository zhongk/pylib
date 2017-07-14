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
			self.__lock = threading.Lock()
			self.__file = file + '.lock'
			self.__fd = open(self.__file, 'w')
		
		def __lockf(self, lock_op):
			try:
				if self.__fd.closed:
					self.__fd = open(self.__file, 'w')
				fcntl.lockf(self.__fd, lock_op)
			except:
				pass
		
		def __enter__(self):
			self.__lock.acquire()
			self.__lockf(fcntl.LOCK_EX)
		
		def __exit__(self, exc_type, exc_val, exc_tb):
			self.__lockf(fcntl.LOCK_UN)
			self.__lock.release()
	
	def __init__(self, path):
		self.meta_file = '%s/Fmq.sdb'%path
		
		self.meta = sqlite3.connect(self.meta_file)
		self.meta.execute('PRAGMA synchronous = OFF')
		self.meta.execute('PRAGMA cache_size = 8000')
		self.meta.execute('PRAGMA case_sensitive_like = 1')
		self.meta.execute('PRAGMA temp_store = MEMORY')
		self.meta.execute('CREATE TABLE IF NOT EXISTS queue_meta(name PRIMARY KEY, partitions, backup_hours, m_interval)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS queue_logs(log_file PRIMARY KEY, queue, partition, timestamp)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS consume_logs(queue, group_id, partition, log_file, offset)')
		self.meta.execute('CREATE TABLE IF NOT EXISTS consume_registry(queue, group_id, partition, pid)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_q_logs ON queue_logs(queue, timestamp)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_c_logs ON consume_logs(queue, group_id)')
		self.meta.execute('CREATE INDEX IF NOT EXISTS idx_c_reg ON consume_registry(queue, group_id)')
		
		self.lock = _Metadata._Lock(self.meta_file)
	
	def get_queue(self, name):
		c = self.meta.cursor()
		c.execute('SELECT name, partitions, backup_hours, m_interval FROM queue_meta WHERE name=?', (name,))
		q_meta = c.fetchone()
		if not q_meta:
			return None
		return dict(zip(('name', 'partitions', 'backup_hours', 'm_interval'), q_meta))
	
	def create_queue(self, name, **kws):
		q_info = self.get_queue(name)
		if not q_info:
			partitions = kws.get('partitions', 1)
			backup_hours = kws.get('backup_hours', 48)
			m_interval = kws.get('m_interval', 5)
			assert(partitions>0 and backup_hours>0)
			assert(0<m_interval<=60 and 60%m_interval==0)
			c = self.meta.cursor()
			c.execute('INSERT INTO queue_meta(name, partitions, backup_hours, m_interval) VALUES(?, ?, ?, ?)',
			          (name, partitions, backup_hours, m_interval))
			self.meta.commit()
			q_info = dict(name=name, partitions=partitions, backup_hours=backup_hours, m_interval=m_interval)
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
	
	def get_last_log(self, queue_name, partition):
		c = self.meta.cursor()
		c.execute('SELECT log_file, timestamp FROM queue_logs'
		          ' WHERE queue=? AND partition=?'
		          ' ORDER BY timestamp DESC', (queue_name, partition))
		return c.fetchone()
	
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
		self.log_file = [dict(fd=None, name='', timestamp=0) for _ in range(partitions)]
		self._messages = [[] for _ in range(partitions)]
		self.__total_sends = [0]*partitions
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
			partition = min(zip(self.__total_sends, range(partitions)))[1]
		self._messages[partition].append((time.time(), key, message))
		self.__total_sends[partition] += 1
	
	def commit(self):
		if not sum(map(len, self._messages)): return
		interval = self.queue['m_interval']*60
		timestamp = int(time.time())//interval*interval
		
		partitions = []
		for partition in range(self.queue['partitions']):
			if not self._messages[partition]: continue
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
		for message in self._messages[partition]:
			pickle.dump(message, fd)
		fd.flush()
		fcntl.lockf(fd, fcntl.LOCK_UN)
		self._messages[partition].clear()
		return newfile
	
	def cleanup_expired_logs(self):
		with self.__metadata.lock:
			expired_logs = self.__metadata.cleanup_expired_logs(self.queue['name'], self.queue['backup_hours'])
		for name in expired_logs:
			log_file = '%s/%s'%(self.path, name)
			os.remove(log_file)


class Consumer:
	Message = namedtuple('ConsumeMessage', ['queue', 'partition', 'key', 'payload', 'timestamp', 'next'])
	
	def __init__(self, queue_name, group_id, partition=0, path='.', **kws):
		self.__metadata = _Metadata.get_metadata(path)
		self.queue = self.__metadata.get_queue(queue_name)
		if not self.queue:
			raise Error("Queue '%s' not found"%queue_name)
		if not partition in range(self.queue['partitions']):
			raise Error("Out of range of partitions")
		
		with self.__metadata.lock:
			if not self.__metadata.regist_consumer(group_id, queue_name, partition):
				consume_tag = "Cosumer(group_id='{}', queue='{}', partition={})".format(group_id, queue_name, partition)
				raise Error("%s already registry by other consumer"%consume_tag)
		
		self.partition = partition
		self.group_id = str(group_id)
		self.path = '%s/%s'%(path, queue_name)
		self.auto_ack = kws.get('auto_ack', True)
		
		self.log_file = dict(fd=None, name='', offset=0, timestamp=0)
		self.__filelist = deque()
		consume_log = self.__metadata.get_consume_log(self.group_id, self.queue['name'], self.partition)
		if consume_log:
			(self.log_file['name'], self.log_file['offset'], self.log_file['timestamp']) = consume_log
		elif kws.get('poll_latest'):
			interval = 60 * self.queue['m_interval']
			self.log_file['timestamp'] = int(time.time())//interval*interval
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
			                        key=key, payload=message, timestamp=timestamp,
			                        next=(self.log_file['timestamp'], self.log_file['offset']))
		except EOFError:
			pass
		
		self.log_file['fd'].close()
		self.log_file['fd'] = None
		return self.poll()
	
	def commit(self):
		if self.ack_log:
			with self.__metadata.lock:
				self.__metadata.put_consume_log(self.group_id, self.queue['name'], self.partition, *self.ack_log)
				self.__metadata.commit()
			self.ack_log = None
	
	def position(self):
		return (self.log_file['timestamp'], self.log_file['offset'])
	
	def seek(self, position):
		(log_timestamp, offset) = position
		log_files = self.__metadata.get_logs(self.queue['name'], self.partition, log_timestamp)
		if not log_files:
			raise Error('Invalid position(log file not exist)')
		filename, timestamp = log_files.popleft()
		if timestamp != log_timestamp:
			raise Error('Invalid position(log file expired)')
		
		path_name = '%s/%s'%(self.path, filename)
		file_size = os.stat(path_name).st_size
		if offset > file_size:
			raise Error('Invalid position(offset is out of log file)')
		fd = None
		if offset < file_size:
			fd = open('%s/%s'%(self.path, filename), 'rb')
			fd.seek(offset)
			pickle.load(fd)
			fd.seek(offset)
		
		if self.log_file['fd'] and not self.log_file['fd'].closed:
			self.log_file['fd'].close()
		self.log_file = dict(fd=fd, name=filename, offset=offset, timestamp=timestamp)
		self.__filelist = log_files
		self._ack()
		self.commit()
	
	def _ack(self):
		self.ack_log = (self.log_file['name'], self.log_file['offset'])
	
	def __open_nextfile(self):
		if self.__filelist:
			filename, timestamp = self.__filelist.popleft()
		else:
			self.__filelist = self.__metadata.get_logs(self.queue['name'], self.partition, self.log_file['timestamp'])
			if not self.__filelist:
				return None
			filename, timestamp = self.__filelist.popleft()
			
			if timestamp == self.log_file['timestamp']:
				path_name = '%s/%s'%(self.path, filename)
				if self.log_file['offset'] < os.stat(path_name).st_size:
					try:
						fd = open(path_name, 'rb')
						fd.seek(self.log_file['offset'])
						return fd
					except FileNotFoundError:
						self.__filelist.clear()
						return self.__open_nextfile()
				if not self.__filelist:
					return None
				filename, timestamp = self.__filelist.popleft()
		
		try:
			fd = open('%s/%s'%(self.path, filename), 'rb')
			self.log_file['name'], self.log_file['offset'], self.log_file['timestamp'] = filename, 0, timestamp
			return fd
		except FileNotFoundError:
			self.__filelist.clear()
			return self.__open_nextfile()


class MultipleConsumer:
	def __init__(self, group_id, path='.', **kws):
		self.group_id, self.__path = group_id, path
		self.__metadata = _Metadata.get_metadata(path)
		self.poll_timeout = kws.get('poll_timeout', 0.1)
		self._messages = {}
		self._pconsumers = {}
	
	def add_consumer(self, queue_name, partitions=None, **kws):
		queue = self.__metadata.get_queue(queue_name)
		if not queue:
			raise Error("Queue '%s' not found"%queue_name)
		
		n = queue['partitions']
		if partitions is None:
			partitions = set(range(n))
		elif not partitions.issubset(range(n)):
			raise Error("Out of range of partitions")
		
		for partition in partitions:
			queue_partition = (queue_name, partition)
			assert(not self._pconsumers.get(queue_partition))
			self._messages[queue_partition] = (None, 0)
			self._pconsumers[queue_partition] = \
				Consumer(queue_name, self.group_id, partition, self.__path, auto_ack=False, **kws)
	
	def __iter__(self):
		return self
	
	def __next__(self):
		message = self.poll()
		if message is None:
			raise StopIteration
		return message
	
	def poll(self):
		fetched = []
		for queue_partition, (message, time_nodata) in self._messages.items():
			if not message:
				now = time.time()
				if now - time_nodata > self.poll_timeout:
					message = self._pconsumers[queue_partition].poll()
					if not message:
						self._messages[queue_partition] = (None, now)
			if message:
				self._messages[queue_partition] = (message, 0)
				fetched.append((message.timestamp, queue_partition))
		
		if not fetched: return None
		queue_partition = min(fetched)[1]
		message = self._messages[queue_partition][0]
		self._pconsumers[queue_partition]._ack()
		self._messages[queue_partition] = (None, 0)
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
	
	c02 = MultipleConsumer(group_id=group_id, path=path)
	c02.add_consumer(queue_name, partitions={0,2})
	for m in c02:
		print(m)
	c02.commit()
