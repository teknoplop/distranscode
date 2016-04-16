#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
import logging
import os
import sys
import subprocess
import traceback
from os.path import join, getsize
import TranscodeTask_pb2
from mutagen.flac import FLAC
from mutagen.id3 import ID3, TIT2, TRCK, TALB, TPE1, TPE2, APIC, COMM, TCMP
from subprocess import Popen, PIPE, STDOUT
import curses
import threading
import locale
from google.protobuf import text_format
import multiprocessing

from FlacTrack import FlacTrack
from Mp3Track import Mp3Track

# would get this from a .ini or DB (or some clever persisted object serialisation)
# especially to keep tasker and worker in sync
# is there anyway to bind these keys with their consumers at compile/link time?
_appConfig = \
{
	"logging.name":"flac2mp3tasks", 
	"logging.path":"/tmp",
	"rabbit.taskQ":"flac2mp3tasks",
	"rabbit.deadQ":"flac2mp3errors",
        "rabbit.host":"192.168.1.11", # TODO: make a cmd line option
	"transcode.flacBin":u'/usr/bin/flac',
	"transcode.lameBin":u'/usr/bin/lame', # or /usr/bin/local/lame
	"transcode.destRoot":u"/muzak/mp3/albums",
	"transcode.srcRoot":u"/muzak/flac/albums",
	"status.stderrFile":"/tmp/stderr.txt",
	"main.workerCount":1,

}

#
# Service Locator 
# Just a dictionary wrapper for now but this is the singleton for the application
# TODO: make re-entrant? In C++ a const ref i/f could avoid this
# TODO: need to avoid inter-service dependencies and consider shutdown order
#
class ServiceLocator( ):
	CONFIG = object()
	LOGGER = object()
	STATUSCOLLECTOR = object()
	MSGTASKER = object()
	MSGSEND = object()

	def __init__( self ):
		self._svc = {}
		self._svc[ ServiceLocator.CONFIG ] = _appConfig

	def register( self, name, svcClass ):
		self._svc[ name ] = svcClass( self.config() )

	def locate( self, name ):
		return self._svc[ name ]

	def config( self ):
		return self._svc[ ServiceLocator.CONFIG ]

	def shutdown( self ):
		for k, s in self._svc.items():
			if ( not k == ServiceLocator.CONFIG ):
				s.shutdown()

#
# Start of Services 
#

#
# Loggger - expose a subset of the pyrthon logger
#
class Logger:
	def __init__ ( self, cfg ):
		log = logging.getLogger( cfg[ "logging.name" ] )
		hdlr = logging.FileHandler( os.path.join( cfg[ "logging.path" ], cfg[ "logging.name" ] ) )
		hdlr.setFormatter( logging.Formatter('%(asctime)s %(levelname)s %(threadName)s %(message)s') )
		log.addHandler(hdlr)
		log.setLevel(logging.DEBUG)
		self._log = log

	def shutdown( self ):
		pass

	def warning( self, *args, **kwargs ):
		return self._log.warning( *args, **kwargs )

	def info( self, *args, **kwargs ):
		return self._log.info( *args, **kwargs )

	def debug( self, *args, **kwargs ):
		return self._log.debug( *args, **kwargs )

	def exception( self, *args, **kwargs ):
		return self._log.exception( *args, **kwargs )


# maybe the status collectors should be pulling info from the workers,
# rather than the workers pushing them

#
# Stdout Status - will mostly print what's in the logfile onto the console
#
class StdoutStatus():
	def __init__( self, cfg ):
		self._enc = locale.getpreferredencoding()
		self._lock = threading.Lock()

	def shutdown( self ):
		pass

	def createdThread( self ):
		with self._lock:
			print "Thread created {0}".format( threading.currentThread().name )

	def startingTask( self, task ):
		with self._lock:
			print "Thread {0} starting task {1}".format( 
				threading.currentThread().name,
				task.inputName.encode( locale.getpreferredencoding() ) )

	def completedTask( self, task ):
		with self._lock:
			print "Thread {0} completed task {1}".format(
				threading.currentThread().name,
				task.inputName.encode( locale.getpreferredencoding() ) )

	def failedTask( self ):
		with self._lock:
			print "Thread {0} error".format( threading.currentThread().name )

	def printStatus( self ):
		pass

	def userTerminated( self ):
		print "User requested terminate"

#
# Ncurses Status
#
class WorkerStatus():
	def __init__( self, cfg ):
		self._enc = locale.getpreferredencoding()
		self._statusDict = {}
		self._successCount = {}
		self._failedCount = {}
		self._lock = threading.Lock()
		self._screen = curses.initscr()
		self._userTerm = False
		curses.curs_set(0)

		# redirect stderr if using ncurse display
		sys.stderr.flush()
		err = open( cfg[ "status.stderrFile" ], 'a+', 0 ) 
		os.dup2( err.fileno(), sys.stderr.fileno() )
		locale.setlocale( locale.LC_ALL, "" ) # allow curses to display utf-8. black magic. 

	def shutdown( self ):
		 curses.endwin()

	def printThreadStatus( self, row, tKey ):

		taskStatus = ""
		task = self._statusDict[ tKey ]
		if ( task is None ):
			taskStatus = u"Idle"
		else:
			taskStatus = u"{0:05d} {1}".format( 
				task.tid,
				task.inputName )

		status = u" {0:>6} | {1:5d} | {2:7d} | {3}".format( 
			tKey,
			self._failedCount[ tKey ],
			self._successCount[ tKey ],
			taskStatus )

		self._screen.addstr( row, 0, status.encode( self._enc ) )

	def printHeader( self ):
		terminateStatus = ""
		if (self._userTerm):
			terminateStatus = "Terminating"

		self._screen.addstr( 0, 0, " Thread | Error | Success | Task {0}".format( terminateStatus ) )

		l = "--------+-------+---------+"
		self._screen.addstr( 1, 0, l )

		for x in range( len(l), self._screen.getmaxyx()[1] ):
			self._screen.addch( 1, x, '-')
		return 2

	def printStatus( self ):
		self._screen.clear()

		headerRowCount = self.printHeader()      

		with self._lock:
			for row, tKey in  enumerate( sorted( self._statusDict.iterkeys() ) ):
				self.printThreadStatus( row + headerRowCount, tKey )

		self._screen.refresh()

	def createdThread( self ):
		n = threading.currentThread().name
		
		with self._lock:
			self._statusDict[ n ] = None
			self._successCount [ n ] = 0
			self._failedCount[ n ] = 0

	def startingTask( self, task ):
		tName = threading.currentThread().name

		with self._lock:
			self._statusDict[ tName ] = task

	def completedTask( self, task ):
		tName = threading.currentThread().name

		with self._lock:
			self._successCount[ tName ] += 1
			self._statusDict[ tName ] = None

	def failedTask( self ):
		tName = threading.currentThread().name

		with self._lock:
			self._failedCount[ tName ] += 1
			self._statusDict[ tName ] = None

	def userTerminated( self ):
		self._userTerm = True 
#
# This is an issue because the API may impose threading restrictions which makes a generic
# model difficult
# Maybe the Async API makes it easier, but not all APIs offer that either
#
class RabbitMQTasker():
	def __init__( self, cfg ):
		self._enc = locale.getpreferredencoding()
		self._statusDict = {}
		self._successCount = {}
		self._failedCount = {}
 
		self._inq = cfg[ "rabbit.taskQ" ]
		self._dlq = cfg[ "rabbit.deadQ" ]
		self._host  = cfg[ "rabbit.host" ]
		self._appTarget = {}
		self._bind = {}

	def shutdown( self ):
		for k, v in iter( self._bind.items() ):
			( connection, channel, target ) = v
		self._bind.clear()

	def dispatchThreadTask( self, target ):

		try:
			connection = pika.BlockingConnection( pika.ConnectionParameters( host=self._host) )

			channel = connection.channel()

			self._bind[ threading.currentThread().name ] = ( connection, channel, target )

			channel.queue_declare( queue=self._inq )
			channel.queue_declare( queue=self._dlq )
			channel.basic_qos( prefetch_count=1 )
			channel.basic_consume(  self.onRabbitMsg, 
									queue=self._inq, 
									no_ack=False, 
									consumer_tag="transCoder_{0}".format( threading.currentThread().name ) )
				
 
			# how do i break into this?
			channel.start_consuming()
		except AMQPConnectionError:
			pass

	def onRabbitMsg( self, ch, method, properties, body ):
		connection, channel, target = self._bind[ threading.currentThread().name ]
		try:
			target( self, body )
		finally:
		   ch.basic_ack( delivery_tag = method.delivery_tag )

	def putDeadLetter( self, msg ):
		connection, channel, target = self._bind[ threading.currentThread().name ]
		channel.basic_publish( exchange='', routing_key=self._dlq, body=msg )

	def disconnect( self ):
		for k, v in iter( self._bind.items() ):
			( connection, channel, target ) = v

		try:
			connection.close()
		except:
			pas

#
# End of services
#




#
# Task converter
#
class TaskConvertor():
	def __init__ ( self, svc ):
		self._log          = svc.locate( ServiceLocator.LOGGER )
		self._workerStatus = svc.locate( ServiceLocator.STATUSCOLLECTOR )

		cfg = svc.config()
		self._destRoot = cfg[ "transcode.destRoot" ]
		self._flacBin  = cfg[ "transcode.flacBin" ]
		self._lameBin  = cfg[ "transcode.lameBin" ] 
		self._srcRoot  = cfg[ "transcode.srcRoot" ]



	def MkDirP( self, d ):
		try:
			if not os.path.exists ( d ):
				os.makedirs ( d )
		except OSError:
			pass

	def bashEsc( self, s ):
		return s.replace("$", "\$").replace("'", "'\\''")

	def processTask( self, task ):
		uInputName = task.inputName

		# update screen
		self._log.debug( u"Starting task {0:04d} {1}".format( task.tid, uInputName ) )
		self._workerStatus.startingTask( task )

			
		inputBase, inputExtension = os.path.splitext( uInputName )
		mp3Path = os.path.join( self._destRoot, task.commonPath )
		dstFile =  os.path.join( mp3Path, inputBase + u'.mp3' )

		self.MkDirP( mp3Path.encode( 'utf-8') )

		# TODO: create a pipe so SrcTrack and DestTrack handle their codecs internally
		flacPath = os.path.join( self._srcRoot, task.commonPath, task.inputName ) # watch out for leading / on the 2nd/3rd params
		self._log.debug( flacPath )

		cmd = u"/bin/bash -c \"set -o pipefail; {0} --apply-replaygain-which-is-not-lossless --silent -d -c '{1}' | {2} -V2 --vbr-new --add-id3v2 --silent - '{3}'\"".format( 
			self._flacBin, 
			self.bashEsc( flacPath ), 
			self._lameBin, 
			self.bashEsc( dstFile ) )
		
		#self._log.debug( cmd )

		p = subprocess.Popen( cmd.encode('utf-8'), shell=True, stdout=subprocess.PIPE )

		stdout_value = p.communicate()[0]

		retcode = p.poll()

		if retcode:
			raise Exception( u'Bash call failed {0}'.format( retcode ) )

		dstTrack = Mp3Track( dstFile )
		srcTrack = FlacTrack( flacPath )

		dstTrack.UpdateMetadata( srcTrack )


		self._log.debug( u"Transcoded {0:04d} {1}".format( task.tid, uInputName ) )
		self._workerStatus.completedTask( task )
		self._log.debug( u"Completed task {0:04d} {1}".format( task.tid, uInputName ) )

	def onTask( self, channel, body ):
		try:
			if type(body) == unicode:
				data = bytearray(body, "utf-8")
				body = bytes(data)

			task = TranscodeTask_pb2.TranscodeTask()
			task.ParseFromString( body )
			self.processTask( task )
		except:
			dumpTask = text_format.MessageToString( task, as_utf8=True, as_one_line = True );
			self._log.exception(  'Failed to process task: {0}'.format( dumpTask ) )
			self._workerStatus.failedTask()

			# todo message retries, then place on error queue
 
			channel.putDeadLetter( dumpTask )


#
# Worker Threads
#
class TaskWorker ( threading.Thread ):
	def __init__ ( self, threadCount, svc ):
		threading.Thread.__init__( self, name='W{0:02d}'.format( threadCount ) )
		self.Daemon = True
		self._workerStatus = svc.locate( ServiceLocator.STATUSCOLLECTOR )
		self._converter = TaskConvertor( svc )
		self._tasker = svc.locate( ServiceLocator.MSGTASKER )
		self._log = svc.locate( ServiceLocator.LOGGER )

	def run ( self ):
		try:
			self._workerStatus.createdThread()
			self._log.info( "Thread Started {0}".format( threading.currentThread().name ) )
			self._tasker.dispatchThreadTask( self._converter.onTask )
		except:
			self._log.exception( str(sys.exc_info()[0]) )

				
# ought to be two different classes to get rid of the if ( self._workerCount > 1 )
class WorkerPool():
	def __init__ ( self, svc ):
		self._workerStatus = svc.locate( ServiceLocator.STATUSCOLLECTOR )
		self._workerCount = svc.config()[ "main.workerCount" ]
		if ( self._workerCount == 0 ):
			self._workerCount = multiprocessing.cpu_count()
		self._tasker = svc.locate( ServiceLocator.MSGTASKER )
		self._log = svc.locate( ServiceLocator.LOGGER )

		if ( self._workerCount > 1 ):
			self._log.info( "WorkerPool creating pool with {0:d} threads".format( self._workerCount ) )
			self._threads = {}

			for w in range( self._workerCount ):
				t = TaskWorker(  w + 1, svc )
				self._threads[ w ] = t
				t.setDaemon( True )
				t.start()
		else:
			self._log.info( "WorkerPool using main thread {0}".format( threading.currentThread().name ) )

	def shutdown( self ):
		pass

	def dispatch( self ):
		if ( self._workerCount > 1 ):
			completedThreads = {}

			while len( completedThreads ) < len( self._threads ):
				for i, t in iter( self._threads.items() ):
					try:

						self._workerStatus.printStatus()

						if t.isAlive():
							t.join(1)
						else:
							completedThreads[ i ] = t
					except KeyboardInterrupt:
						self._log.info( "Caught KeyboardInterrupt" )
						self._tasker.disconnect()
						self._workerStatus.userTerminated()
						print "KeyboardInterrupt"

			if len( completedThreads ) == len( self._threads ):
				print "All Threads complete"

		else:
			tasker = svc.locate( ServiceLocator.MSGTASKER )
			tc = TaskConvertor( svc )
			tasker.dispatchThreadTask( tc.onTask )

#
# Main
#
svc = ServiceLocator()
if ( svc.config()[ "main.workerCount" ] != 1 ):
	svc.register( ServiceLocator.STATUSCOLLECTOR, WorkerStatus )
else:
	svc.register( ServiceLocator.STATUSCOLLECTOR, StdoutStatus )


svc.register( ServiceLocator.LOGGER, Logger )
svc.register( ServiceLocator.MSGTASKER, RabbitMQTasker )

threading.currentThread().name = "M00"    

wp = WorkerPool( svc )
wp.dispatch()

wp.shutdown()
svc.shutdown()




















