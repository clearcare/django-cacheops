VERSION = (1, 3, 1)
__version__ = '.'.join(map(str, VERSION if VERSION[-1] else VERSION[:2]))

from .simple import *  # NOQA
from .query import *  # NOQA
from .invalidation import *  # NOQA

install_cacheops()
