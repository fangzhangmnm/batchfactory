from PIL import Image
from io import BytesIO
import numpy as np

from batchfactory.lib.base64_utils import encode_bytes, decode_bytes, encode_image, decode_image, encode_ndarray, decode_ndarray

def download_if_missing(url, path, binary=False, headers=None):
    import os, requests
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        mode = "wb" if binary else "w"
        with open(path, mode, encoding=None if binary else "utf-8") as f:
            f.write(response.content if binary else response.text)
    mode = "rb" if binary else "r"
    with open(path, mode, encoding=None if binary else "utf-8") as f:
        return f.read()

def download_alice(path="alice.txt"):
    url = "https://www.gutenberg.org/files/11/11-0.txt"
    return download_if_missing(url, path, binary=False)

def download_meow(path="./data/examples/meow.ogg"):
    url = "https://upload.wikimedia.org/wikipedia/commons/0/0c/Meow_domestic_cat.ogg"
    headers = {"User-Agent": "Mozilla/5.0"}
    return download_if_missing(url, path, binary=True, headers=headers)

def download_lenna(path="./data/examples/lenna.png"):
    url = "https://upload.wikimedia.org/wikipedia/en/7/7d/Lenna_%28test_image%29.png"
    content = download_if_missing(url, path, binary=True)
    img = Image.open(BytesIO(content)).convert("RGB")
    img.info["desc"] = "Lenna original"
    return img

def download_anime_style(path="./data/examples/anime_eye.png"):
    url = "https://upload.wikimedia.org/wikipedia/commons/thumb/b/bb/Anime_eye.svg/1152px-Anime_eye.svg.png"
    content = download_if_missing(url, path, binary=True)
    img = Image.open(BytesIO(content)).convert("RGB")
    img.info["desc"] = "Anime style"
    return img



def test_encode_decode_bytes():
    data = b"Hello, World!"
    encoded = encode_bytes(data)
    decoded = decode_bytes(encoded)
    assert decoded == data, "Decoded bytes do not match original data."

def test_encode_decode_ndarray():
    array = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float16)
    encoded = encode_ndarray(array)
    decoded = decode_ndarray(encoded)
    assert np.array_equal(decoded, array), "Decoded ndarray does not match original array."

def test_encode_decode_image():
    img = download_lenna()
    encoded = encode_image(img)
    decoded = decode_image(encoded)
    assert decoded.size == img.size, "Decoded image size does not match original."
    assert decoded.mode == img.mode, "Decoded image mode does not match original."

def test_encode_decode_audio_bytes():
    meow = download_meow()
    encoded = encode_bytes(meow)
    decoded = decode_bytes(encoded)
    assert decoded == meow, "Decoded audio bytes do not match original data."

