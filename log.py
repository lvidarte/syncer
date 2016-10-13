"""
Author: Leo Vidarte <http://nerdlabs.com.ar>

This is free software,
you can redistribute it and/or modify it
under the terms of the GPL version 3
as published by the Free Software Foundation.

"""

import logging
import json
from datetime import datetime
import config


class JsonFormatter(logging.Formatter):
    def format(self, record):
        _fmt = '%Y-%m-%d %H:%M:%S.%f'
        _datetime = datetime.fromtimestamp(record.created).strftime(_fmt)
        record.msg = json.dumps({
            'date': _datetime,
            'level': record.levelname,
            'message': record.msg
        })
        return super(JsonFormatter, self).format(record)

formatter = JsonFormatter()

console_handler = logging.StreamHandler()
console_handler.setLevel(config.LOG_LEVEL)
console_handler.setFormatter(formatter)

logger = logging.getLogger('syncer')
logger.setLevel(config.LOG_LEVEL)
logger.addHandler(console_handler)

