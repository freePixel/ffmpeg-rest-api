import subprocess
from dataclasses import dataclass
import subprocess
import logging
@dataclass
class CompressVideoConfig:
    outpath: str
    location: str
    factor: int
    framerate: int
    quality: str


def isInteger(number: int):
    try:
        int(number)
        return True
    except Exception:
        return False

def buildCompressionCommand(location: str, factor: int, framerate: int, outpath: str, quality: str):

    if not isInteger(factor) or not isInteger(framerate):
        raise Exception("Cannot build command, expected int parameters")
    
    ypixels = 720
    if quality == "1080p":
        ypixels = 1080

    return f'ffmpeg -i {location} -fpsmax {str(framerate)} -crf {str(factor)} -c:v libx264 -filter:v scale="trunc(oh*a/2)*2:{str(ypixels)}" -y -threads 1 {outpath}'
    

def compressVideo(config: CompressVideoConfig):
    
    command = buildCompressionCommand(config.location, config.factor, config.framerate, config.outpath, config.quality)

    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode != 0:
        raise Exception(f"Unable to compress video, {result.stderr}")
    
    logging.debug(result.stdout)