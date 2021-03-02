from filemanager import *

def test_create_file_name_without_prefix():
    filename = createFileName("", "testFile.jpg")

    assert filename == "testFile.jpg"

def test_create_file_name_with_prefix():
    filename = createFileName("prefixTest", 'testFile.jpg')

    assert filename == "prefixTest-testFile.jpg"

def test_chunk_list_by_parts():
    chunksGenerator = chunkList([1, 2, 3, 4, 5, 6, 7, 8 , 9])

    assert next(chunksGenerator) == [1, 2, 3 ]
    assert next(chunksGenerator) == [4, 5, 6 ]
    assert next(chunksGenerator) == [7, 8, 9 ]
