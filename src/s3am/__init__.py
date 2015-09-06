# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sys

me = os.path.basename( sys.argv[ 0 ] )

log = logging.getLogger( __name__ )

logging.basicConfig( level=logging.WARN,
                     format="%(asctime)-15s %(module)s(%(process)d) %(message)s" )


class UserError( Exception ):
    """
    An exception that doesn't cause a stack trace to be printed.
    """
    pass


class WorkerException( Exception ):
    """
    An exception where we let other workers finish
    """
    pass
