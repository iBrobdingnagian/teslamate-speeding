import os
import sys

here = os.path.dirname(__file__)
for sub in ("speeding", "alerts"):
    sys.path.insert(0, os.path.join(here, "..", sub))
