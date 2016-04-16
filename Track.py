import abc
import os

class Track( object ):
	__metaclass__ = abc.ABCMeta

	_commTag = u'srcMd5'
	_commLang = u'eng'

	def __init__(self, pathName ):
		self._pathName = pathName

	def CommentMd5( self, line, k ):
		kv = line.split('=')

		if len(kv) == 2:
			if kv[0] == k:
				return kv[1]

			return  None

	@property
	def PathName(self):
		return self._pathName

	@property
	def Name(self):
		h, t = os.path.split(self.PathName)
		return t

	@property
	def Path(self):
		h, t = os.path.split(self.PathName)
		return h

	@property
	def BaseName(self):
		return os.path.basename(self.PathName)

	@property
	def ExtensionlessBaseName(self):
		r, e = os.path.splitext( self.BaseName )	
		return r

	@abc.abstractproperty
	def Album(self):
		return None

	@abc.abstractproperty
	def AlbumArtist(self):
		return None

	@abc.abstractproperty
	def Artist(self):
		return None

	@abc.abstractproperty
	def Title(self):
		return None

	@abc.abstractproperty
	def TrackNumber(self):
		return None

	@abc.abstractproperty
	def AudioMD5(self):
		return None	

	@abc.abstractproperty
	def Wife(self):
		return None

	@abc.abstractproperty
	def IsPartOfCompilation(self):
		return None

	@abc.abstractproperty
	def AlbumArtwork(self):
		return None		
