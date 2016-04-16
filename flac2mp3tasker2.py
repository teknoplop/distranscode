#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pika
import TranscodeTask_pb2
import os
import sys
import getopt
from FlacTrack import FlacTrack
from Mp3Track import Mp3Track

from os.path import join, getsize
import logging
import traceback


_pathDst = "/mp3"
_pathSrc = "/albums"
_taskQ = 'flac2mp3tasks'
_taskid = 0

def main(argv):

	logging.getLogger('pika').setLevel(logging.DEBUG)

	connection = pika.BlockingConnection( pika.ConnectionParameters( 'tonga' ) )
	channel = connection.channel()
	channel.queue_declare( queue=_taskQ )

	for root, dirs, files in os.walk( _pathSrc, followlinks=True ):
		if IsAlbumDir( files, root ):
			print "AlbumDir:", root

			flacFiles = set()
			partOfCompilation = False

			for f in files:
				if IsFlacFile( f ) :
					flacFile = FlacTrack(os.path.abspath( os.path.join( root, f ) ) )
					flacFiles.add( flacFile )

					partOfCompilation |= flacFile.IsPartOfCompilation

			if partOfCompilation:
				print "  Compilation"

			updated = 0
			queued = 0
			missing = 0
			exception = 0
			md5 = 0
			forceUpdate = False
			for src in flacFiles:

				queueTrack = False
                                

				dstPath = JoinDestPath( _pathDst, src, _pathSrc, "mp3" )

				if not os.path.isfile(dstPath):
					# no dstTrack so needs creating
					# print "    Mp3 missing", dstPath
					missing += 1
					queueTrack = True

					#print "missing", dstPath
				else:
					try:
					# TODO - make dest type agnostic to support OGG
						dst = Mp3Track( dstPath )

						# if the flac checksum matches but the other meta data doesn't then update here
						# instead of queuing

						if TranscodeRequired( src, dst ):
							#print "MD5 mismatch"
							queueTrack = True
							md5 += 1

						elif forceUpdate or not MetadataIsEqual( src, dst ):
							# update metadata directly if stale
							updated +=1 
							dst.UpdateMetadata ( src )
					except Exception as ex:
						traceback.print_exc()
						queueTrack = True
						exception += 1

				if queueTrack:
					queued += 1
					taskBytes = MakeTaskBytes( src, _pathSrc )
					channel.basic_publish( exchange='', routing_key=_taskQ, body=taskBytes )					
			
                        if queued or updated:
                                print "    missing", missing, "exception", exception, "md5", md5

			if queued:
				print "    queued", queued
			elif updated:
				print "    updated", updated

	channel.close()

	connection.close()

def TranscodeRequired( srcTrack, dstTrack ):
	try:
		return srcTrack.AudioMD5 != dstTrack.AudioMD5
	except Exception as e:
		traceback.print_exc()
		return True


def MetadataIsEqual( srcTrack, dstTrack ):

	try:
		if srcTrack.Title != dstTrack.Title:
			print "    Title"
			return False

		if srcTrack.TrackNumber != dstTrack.TrackNumber :
			print "    TrackNumber"
			return False

		if srcTrack.Album != dstTrack.Album :
			print "    Album"
			return False
		
		if srcTrack.Artist != dstTrack.Artist :
			print "    Artist"
			return False
		
		if srcTrack.AlbumArtist != dstTrack.AlbumArtist :
			print "    AlbumArtist"
			return False
		
		if srcTrack.Wife != dstTrack.Wife :
			print "    Wife"
			return False
		
		if srcTrack.IsPartOfCompilation != dstTrack.IsPartOfCompilation:
			print "    Comp"
			return False

		srcArtwork = srcTrack.AlbumArtwork
		dstArtwork = dstTrack.AlbumArtwork

		if srcArtwork is None and dstArtwork is not None:
			#dst artwork needs to be cleared
			print "    Clear artwork"
			return False 

		if srcArtwork is not None and dstArtwork is None:
				# srcArtrwok needs adding
				print "    Set artwork"
				return False

		if srcArtwork is not None and dstArtwork is not None:
			#md5 can be checked	
			if srcArtwork.Checksum != dstArtwork.Checksum:
				print "    Artwork"
				return False
	except Exception as e:
		#traceback.print_exc()
		return False

	return True


def JoinDestPath( root, src, srcRoot, extenstion ):
	return os.path.join( root, src.Path[ len(srcRoot) + 1 : ], src.ExtensionlessBaseName ) + "." + extenstion


def MakeTaskBytes( src, srcRoot ):
	global _taskid
	
	task = TranscodeTask_pb2.TranscodeTask()
	
	task.tid        = _taskid
	task.inputName  = unicode( src.Name, "utf-8" ) #src.Name
	task.inputPath  = u"" #inputPath #unicode( inputPath, "utf-8" )
	task.commonPath = unicode( src.Path[ len(srcRoot) + 1 : ],  "utf-8" )
	task.artHash    = u""

	task.partOfCompilation = src.IsPartOfCompilation
	_taskid += 1

	return task.SerializeToString()


def IsFlacFile( f ):
	return os.path.splitext( f )[1].lower() == ".flac"

def IsAlbumDir( files, root ):
	for f in files:
		if IsFlacFile( f ):
			return True

	return False;

if __name__ == "__main__":
	main(sys.argv)
