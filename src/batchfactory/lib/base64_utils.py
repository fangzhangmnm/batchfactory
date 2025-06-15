from typing import TYPE_CHECKING, List, Dict
import base64
from io import BytesIO

import PIL.Image
if TYPE_CHECKING:
    import numpy
    import PIL

def encode_bytes(data:bytes, **kwargs) -> Dict:
    return {
        "format": "bytes",
        "bytes": base64.b64encode(data).decode('utf-8'),
        **kwargs
    }

def decode_bytes(item:Dict) -> bytes:
    return base64.b64decode(item["bytes"].encode('utf-8'))
def _get_numpy():
    try: import numpy as np; return np
    except ImportError: raise ImportError("Numpy is required for base64 encoding/decoding numpy.ndarray, please install it.")
def encode_ndarray(array:List|'numpy.ndarray') -> Dict:
    np = _get_numpy()
    array = np.asarray(array) if not isinstance(array, np.ndarray) else array
    return encode_bytes(
        array.tobytes(),
        format="numpy.ndarray",
        dtype=str(array.dtype),
        shape=list(array.shape)
    )
def decode_ndarray(data:Dict) -> 'numpy.ndarray':
    np = _get_numpy()
    return np.frombuffer(decode_bytes(data), dtype=data["dtype"]).reshape(tuple(data["shape"]))
def _get_PIL():
    try: import PIL; return PIL
    except ImportError: raise ImportError("Pillow is required for base64 encoding/decoding PIL.Image, please install it.")
def encode_image(image:'PIL.Image.Image') -> Dict:
    buffer = BytesIO()
    image.save(buffer, format='PNG')
    return encode_bytes(
        buffer.getvalue(),
        format="PIL.Image",
        mode=image.mode,
        size=image.size
    )
def decode_image(data:Dict) -> 'PIL.Image.Image':
    PIL = _get_PIL()
    buffer = BytesIO(decode_bytes(data))
    return PIL.Image.open(buffer).convert(data["mode"])
def resize_image_downscale(image: 'PIL.Image.Image', max_width, max_height):
    """Resize an image downscale to fit within the specified maximum width and height."""
    PIL = _get_PIL()
    original_width, original_height = image.size
    if original_width <= max_width and original_height <= max_height:
        return image
    ratio = min(max_width / original_width, max_height / original_height)
    new_size = (int(original_width * ratio), int(original_height * ratio))
    return image.resize(new_size, PIL.Image.LANCZOS)


__all__ = [
    "encode_bytes",
    "decode_bytes",
    "encode_ndarray",
    "decode_ndarray",
    "encode_image",
    "decode_image",
    "resize_image_downscale",
]