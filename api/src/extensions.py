from enum import Enum
import uuid




def isMediaTypeAllowed(type: str):
    return type in ['video/mp4']

def mediaTypeToExtension(type: str):
    if type == 'video/mp4':
        return "mp4"
    
def extractExtension(name: str):

    if type(name) != str:
        return None

    split = name.split(".")

    if len(split) != 2:
        return None
    
    return split[1]

def generateFileNameByMedia(type: str):
    if type =='video/mp4':
        return f"{str(uuid.uuid4())}.mp4"