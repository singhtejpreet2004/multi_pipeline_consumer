"""
Stubs the on-server DeepSort / Entry-Exit modules (normally loaded from
/data/Entry_Exit via sys.path.insert in consumers/consumer.py) so that
`consumers.consumer` can be imported in any environment — no GPU, no CUDA,
and no /data/Entry_Exit directory required.

Without this, `import consumers.consumer` raises ModuleNotFoundError on any
machine that isn't the production GPU server, because generate_detections
and the deep_sort package only exist at /data/Entry_Exit there.

This does NOT stub numpy/opencv/torch/tensorflow/ultralytics/flask_socketio/
pynvml/kafka-python — those are real pip dependencies from requirements.txt
and must be installed for tests to import consumers.consumer successfully.
"""
import sys
import types
from unittest.mock import MagicMock


def _stub_module(name: str, **attrs) -> types.ModuleType:
    if name in sys.modules:
        return sys.modules[name]
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


_stub_module("generate_detections", create_box_encoder=MagicMock(return_value=MagicMock()))

_deep_sort_pkg = _stub_module("deep_sort")
_nn_matching = _stub_module("deep_sort.nn_matching", NearestNeighborDistanceMetric=MagicMock)
_detection = _stub_module("deep_sort.detection", Detection=MagicMock)
_tracker = _stub_module("deep_sort.tracker", Tracker=MagicMock)

_deep_sort_pkg.nn_matching = _nn_matching
_deep_sort_pkg.detection = _detection
_deep_sort_pkg.tracker = _tracker
