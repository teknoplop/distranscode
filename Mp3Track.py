from Track import Track
from AlbumArtwork import AlbumArtwork
import os
import io
from mutagen.id3 import ID3, TIT2, TRCK, TALB, TPE1, TPE2, APIC, COMM, TCMP

class Mp3Track(Track):

	"""Expose canonical metadata from a single MP3 file"""

	_mp3 = None

	def __init__( self, path ):

		""" Load metadata from path """
		#print type (path)
		self._mp3 = ID3( path )
		#self._mp3.pprint()

	@property
	def Album(self):
		return self._mp3["TALB"].text[0]

	@property
	def AlbumArtist(self):
		return self._mp3["TPE2"].text[0]

	@property
	def Artist(self):
		return self._mp3["TPE1"].text[0]

	@property
	def Title(self):
		return self._mp3["TIT2"].text[0]

	@property
	def TrackNumber(self):
		return self._mp3["TRCK"].text[0]

	@property
	def Comment( self ):
		return self._mp3[ "COMM:distranscode:eng" ].text[0]

	def CommentValue( self, key ):
		#for k in self._mp3.keys():
		#	print k

		for f in self.Comment.split(u":"):
			kv = f.split(u"=")
			if len( kv) == 2:
				if kv[0] == key:
					return kv[1]

		return None	

	@property
	def AudioMD5(self):
		return self.CommentValue( u"flacMD5" )

	@property
	def JpegCRC(self):
		return self.CommentValue( u"jpegCRC" )

	@property
	def Wife(self):
		l =  self.CommentValue( u"Wife" )
		return l is not None and l == u"1"

	@property
	def IsPartOfCompilation(self):
		return "TCMP" in self._mp3 and self._mp3["TCMP"][0] == u'1'

	@property
	def AlbumArtwork(self):

		if "APIC:Front cover" in self._mp3:
			cover = self._mp3[ "APIC:Front cover"]

			if cover.mime == u"image/jpeg":
				return AlbumArtwork( io.BytesIO( cover.data ) )
	#		else:
	#			print "no jpeg cover"
	#	else:
	#		print "no cover"

		return None


	def UpdateMetadata( self, srcTrack ):
		self._mp3.delete()
		self._mp3.add( TIT2( encoding=3, text=srcTrack.Title ) )
		self._mp3.add( TRCK( encoding=3, text=srcTrack.TrackNumber ) )
		self._mp3.add( TALB( encoding=3, text=srcTrack.Album ) )
		self._mp3.add( TPE1( encoding=3, text=srcTrack.Artist ) )
		self._mp3.add( TPE2( encoding=3, text=srcTrack.AlbumArtist ) )

		if srcTrack.IsPartOfCompilation:
			self._mp3.add( TCMP( encoding=3, text='1' ) )

		artwork = srcTrack.AlbumArtwork

		jpegCRC = u""
		if artwork is not None:
			jpegCRC = artwork.Checksum
			self._mp3.add( APIC( encoding=3, mime=u'image/jpeg', type=3, desc=u'Front cover', data=artwork.Data ) )

		commentText = u"flacMD5=" + srcTrack.AudioMD5      
		commentText += u":jpegCRC=" + jpegCRC

		commentText += u":Wife="
		if srcTrack.Wife:
			commentText += u"1"
		else:
			commentText += u"0"

		self._mp3.add( COMM( encoding=3, lang=u'eng', desc=u'distranscode', text=commentText ) );

		self._mp3.update_to_v23()
		self._mp3.save(v1=0, v2_version=3)


