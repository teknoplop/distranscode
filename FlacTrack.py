#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Track import Track
from AlbumArtwork import AlbumArtwork
import os
import io
from mutagen.flac import FLAC

class FlacTrack(Track):

	"""Expose canonical metadata from a single FLAC file"""

	def __init__(self, path ):
		super(FlacTrack, self).__init__(path)

		""" Load metadata from path """
		self._flac = FLAC( self.PathName )

	@property
	def Album(self):
		return self._flac["album"][0]

	@property
	def AlbumArtist(self):
		if "album artist" in self._flac.keys():
			return self._flac["album artist"][0]

		#print "No AlbumArtist"
		return self.Artist

	@property
	def Artist(self):
		return self._flac["artist"][0]

	@property
	def Title(self):
#		print type( self._flac["title"][0].text ), self._flac["title"][0].text
		return self._flac["title"][0]	

	@property
	def TrackNumber(self):
		return self._flac["tracknumber"][0]

	@property
	def AudioMD5(self):
		md5 =  unicode( hex(self._flac.info.md5_signature) )
                #print md5
                return md5

	@property
	def Wife(self):
		return "custom1" in self._flac and self._flac["custom1"][0] == u'Wife'

	@property
	def IsPartOfCompilation(self):
		artist = self.Artist.lower()
		albumArtist = self.AlbumArtist.lower()

		return ( artist == 'various' or 
			artist == 'various artists' or 
			albumArtist == 'various' or 
			albumArtist == 'various artists' or 
			not artist == albumArtist )

	@property
	def AlbumArtwork(self):
		# should be a jpeg
		#print "pictures", self._flac.pictures
		if len( self._flac.pictures ) > 0:
			cover = self._flac.pictures[0]

			if cover.mime == "image/jpeg":
				#print cover.data
				return AlbumArtwork( io.BytesIO( cover.data ) )
	#		else:
	#			print "no jpeg cover"
	#	else:
	#		print "no cover"

		#print type (self.Path), self.Path
		#print type (self.Album), self.Album
		
		path = None
		if isinstance(self.Path, unicode):
			path = self.Path.encode('utf-8')
		else:
			path = self.Path
		
		artPath = os.path.join( path, self.Album.encode('utf-8') ) + ".jpg"
		if os.path.isfile(artPath):
			return AlbumArtwork( io.BytesIO( open(artPath).read() ) )

		return None
