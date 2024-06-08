import pytest
import uuid
from src.extensions import isMediaTypeAllowed, mediaTypeToExtension, extractExtension, generateFileNameByMedia

def test_isMediaTypeAllowed():
    # Test valid media type
    assert isMediaTypeAllowed('video/mp4') == True

    # Test invalid media type
    assert isMediaTypeAllowed('audio/mp3') == False

def test_mediaTypeToExtension():
    # Test valid media type
    assert mediaTypeToExtension('video/mp4') == 'mp4'

    # Test invalid media type (function should not return anything)
    assert mediaTypeToExtension('audio/mp3') == None

def test_extractExtension():
    # Test valid file name
    assert extractExtension('video.mp4') == 'mp4'
    
    # Test invalid file name (no extension)
    assert extractExtension('video') == None
    
    # Test invalid file name (more than one period)
    assert extractExtension('video.final.mp4') == None
    
    # Test non-string input
    assert extractExtension(123) == None

def test_generateFileNameByMedia():
    # Test valid media type
    filename = generateFileNameByMedia('video/mp4')
    assert filename.endswith('.mp4')
    
    # Check if the filename starts with a UUID
    uuid_part = filename.split('.')[0]
    try:
        uuid.UUID(uuid_part)
        valid_uuid = True
    except ValueError:
        valid_uuid = False
    assert valid_uuid == True

    # Test invalid media type (function should not return anything)
    assert generateFileNameByMedia('audio/mp3') == None

# Run tests using `pytest` command from the terminal
if __name__ == "__main__":
    pytest.main()