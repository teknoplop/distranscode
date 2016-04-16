from PIL import Image
import hashlib
import io

class AlbumArtwork(object):

	"""Expose checksum of AlbumArtwork JPEG"""


	def __init__(self, jpegBytesIo ):
		Image.open( jpegBytesIo )

		self._jpegIo = jpegBytesIo
		self._md5 = None


	@property
	def Data(self):
		return self._jpegIo.getvalue()

	@property
	def Checksum(self):

		if self._md5 is None:
			m = hashlib.md5()
			m.update( self.Data )
			self._md5 = unicode( m.hexdigest() )

		return self._md5
