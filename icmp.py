import socket
import select
import struct
import array
import time
import collections

def _inner_checksum(packet):
	sum = 0
	if len(packet) & 1:
		packet += b"\0"
	for word in array.array("H", packet):
		sum += (word & 0xffff)
	sum = (sum >> 16) + (sum & 0xffff)
	sum += (sum >> 16)
	return (~sum) & 0xffff

def msg_encode(icmp_type, data, icmp_code=0, icmp_id=0, icmp_seq=0):
	header = struct.pack("!BBHHH", icmp_type, icmp_code, 0, icmp_id, icmp_seq)
	packet = header + data
	checksum = _inner_checksum(packet)
	packet = packet[:2] + struct.pack("H", checksum) + packet[4:]
	return packet
	
icmphdr = collections.namedtuple("icmphdr", ["type", "code", "id", "sequence", "ttl"])
def msg_decode(packet):
	ttl = packet[8]
	icmp_type, icmp_code, checksum, icmp_id, icmp_seq = struct.unpack("!BBHHH", packet[20:28])
	data = packet[28:]
	return data, icmphdr(type=icmp_type, code=icmp_code, id=icmp_id, sequence=icmp_seq, ttl=ttl)
	
class Ping:
	def __init__(self):
		self.__socket = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.getprotobyname("icmp"))
	
	def request(self, dest_host, icmp_id=0, icmp_seq=0, ttl=None):
		data = struct.pack("!d", time.time())
		rest = b"\0" * (56 - len(data))
		packet = msg_encode(8, data+rest, 0, icmp_id, icmp_seq)
		if ttl and ttl>0:
			self.__socket.setsockopt(socket.SOL_IP, socket.IP_TTL, ttl)
		self.__socket.sendto(packet, (dest_host, 1))

	def get_response(self, timeout):
		ready = select.select([self.__socket], [], [], timeout)
		if not ready[0]:
			return 0, dict()
		rcvtime = time.time()
		packet, addr = self.__socket.recvfrom(1024)
		data, info = msg_decode(packet)
		sndtime = struct.unpack("!d", data[:8])[0]
		if info.type == 0:
			return len(packet)-20, \
			       dict(icmp_id=info.id, icmp_seq=info.sequence, ttl=info.ttl, time=rcvtime-sndtime, host=addr[0])
		else:
			return -1, dict(icmp_type=info.type, icmp_code=info.code, host=addr[0])
	
def traceroute(dest_host, max_hops=30, timeout=30):
	reachable = False
	tracepath = []
	tracer = Ping()
	for hops in range(1, max_hops+1):
		tracer.request(dest_host, icmp_seq=hops, ttl=hops)
		sndtime = time.time()
		res, pong = tracer.get_response(timeout)
		if res == 0:
			tracepath.append((hops, "*", 0))
		else:
			tracepath.append((hops, pong["host"], time.time()-sndtime))
			if pong["host"]==dest_host or (res<0 and pong["icmp_type"]==3):
				reachable = (res > 0)
				break
	return reachable, tracepath

if __name__ == "__main__":
	import sys, threading
	
	ping = Ping()
	
	class SendPingThread(threading.Thread):
		def __init__(self, addr_prefix, timeout=None):
			threading.Thread.__init__(self)
			self.addr_prefix = addr_prefix
			self.timeout = timeout or 3
		def run(self):
			for n in range(1, 255):
				host = self.addr_prefix + str(n)
				time.sleep(0.01)
				ping.request(host, 0, n)
			time.sleep(self.timeout)
	
	addr_prefix = "192.168.%d." % int(sys.argv[1])
	sender = SendPingThread(addr_prefix)
	sender.start()
	
	while sender.is_alive():
		res, pong = ping.get_response(0.1)
		if res > 0:
			print("%d bytes from %-14s: icmp_seq=%-3d ttl=%-3d time=%.3f ms" % \
			      (res, pong["host"], pong["icmp_seq"], pong["ttl"], pong["time"]*1000))
	sender.join()
