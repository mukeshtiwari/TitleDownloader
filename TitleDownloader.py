import urllib2, os, socket, Queue, thread, signal, sys, re


class Downloader():

	def __init__( self ):
		self.q = Queue.Queue( 200 )
		self.count = 0 
	


	def downloadurl( self ) :
		#open a file in append mode and write the result ( Improvement think of writing in chunks ) 
		with open('titleoutput.dat', 'a+' ) as file :	
			while True :
				try :
					url = self.q.get( )
					data = urllib2.urlopen ( url , data = None , timeout = 10 )
					regex = re.compile('<title.*>(.*?)</title>' , re.IGNORECASE)
					#Read data line by line and as soon you find the title go out of loop. for title in data. 
					#for r in data:
						#if not r :
						#	raise StopIteration
						#else: 
						#	t = regex.search( r )
						#	if t is not None: break

					title = regex.search( data.read() )
					result =  ', '.join ( [ url , title.group(1) ] )
					data.close()
					file.write(''.join( [ result , '\n' ] ) )
				except urllib2.HTTPError as e:
				       print ''.join ( [ url, '  ', str ( e ) ] ) 
				except urllib2.URLError as e:
					print ''.join ( [ url, '  ', str ( e ) ] )
				except Exception as e :
					print ''.join ( [ url, '  ', str( e )  ] )
			file.close()	

	def createurl ( self ) :

		#check if file exist. If not then create one with default value of 0 bytes read.
		if os.path.exists('bytesread.dat'):
			f = open ( 'bytesread.dat','r')
			self.count = int ( f.readline() )
					           
		else:
			f=open('bytesread.dat','w')
			f.write('0\n')
			f.close()

		#Reading data in chunks is fast but we can miss some sites due to reading the data in chunks( It's worth missing because reading is very fast)
		with open('top-1m.csv', 'r') as file:
			prefix = ''
			file.seek(  self.count * 1024 )
			#you will land into the middle of bytes so discard upto newline
			if ( self.count ): file.readline()	
			for lines in iter ( lambda : file.read( 1024 ) , ''):
				l = lines.split('\n')
				n = len ( l )
				l[0] = ''.join( [ prefix , l[0] ] )
				for i in xrange ( n - 1 ) : self.q.put ( ''.join ( [ 'http://www.', l[i].split(',')[1] ] ) )
				prefix = l[n-1]
				self.count += 1

			
	#do graceful exit from here.
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
			

