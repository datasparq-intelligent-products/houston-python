
import logging
import sys
from .client import Houston
from .commands import start, update, save, ignore, delete, fail, trigger, skip, static_fire

# default logger also logs to stdout
# set 'LOG_NAME' env var to use a different logger
log = logging.getLogger("houston")
log.addHandler(logging.StreamHandler(stream=sys.stdout))
log.setLevel(logging.INFO)
