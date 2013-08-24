import urllib2, socket, Queue, thread, signal, sys, re

class Downloader():

	def __init__( self ):
		self.q = Queue.Queue( 100 )
		self.count = 0
		socket.setdefaulttimeout( 10 )

	def downloadurl( self ) :
		#open a file and write the result ( write in chunks ) 
		with open('titleoutput.dat', 'a+' ) as file :	
			while True :
				try :
					url = self.q.get( )
					data = urllib2.urlopen ( url )
					regex = re.compile('<title.*>(.*?)</title>' , re.IGNORECASE)
					title = regex.search(data.read())
					result =  ', '.join ( [ url , title.group(1) ] )
					data.close()
					file.write(''.join( [ result , '\n' ] ) )
				except urllib2.URLError, e :
					print ''.join ( [ url, '  ', str ( e.code ) ] )
					#print e.read()
				except socket.error:
					print ''.join ( [ url, '  ', 'Could not open socket' ] )
				except :
					print ''.join ( [ url, '  ', 'Something wrong' ] )
				

	def createurl ( self ) :

		#read the number from file. The purpose here to open in read mode is if there is no file then create one
		with open('bytesread.dat','w') as file:
			try :
				self.count = int ( file.readline() )
			except :
				self.count = 0 
			file.close()

		#Reading data in chunks is fast but we can miss some sites due to reading the data in chunks( It's worth missing because reading is very fast
		with open('top-1m.csv', 'r') as file:
			prefix = ''
			file.seek( self.count * 1024 )
			for lines in iter ( lambda : file.read( 1024 ) , ''):
				l = lines.split('\n')
				n = len ( l )
				l[0] = ''.join( [ prefix , l[0] ] )
				for i in xrange ( n - 1 ) : self.q.put ( ''.join ( [ 'http://www.', l[i].split(',')[1] ] ) )
				prefix = l[n-1]
				self.count += 1

	def handleexception ( self , signal , frame) :
		with open('bytesread.dat', 'w') as file:
			print ''.join ( [ 'Number of bytes read ( probably unfinished ) ' , str ( self.count ) ] )
			file.write ( ''.join ( [ str ( self.count ) , '\n' ] ) )
			file.close()
			sys.exit(0)

if __name__== '__main__':
	u = Downloader()
	signal.signal( signal.SIGINT , u.handleexception)
	thread.start_new_thread ( u.createurl , () )
	for i in xrange ( 5 ) :
		thread.start_new_thread ( u.downloadurl , () )
	while True : pass
			

