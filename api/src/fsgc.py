import os
import logging
import datetime
import job

def fileCanBeRemoved(fpath: str, manager: job.JobManager):
    relatedJobs = manager.getRelatedJobs(fpath)

    if len(relatedJobs) == 0:
        return isFileExpired(fpath)
    
    if all(job.isExpired() for job in relatedJobs):
        return True


def isFileExpired(fpath: str):
    date = datetime.datetime.fromtimestamp(os.path.getmtime(fpath))
    if datetime.datetime.now() > date + datetime.timedelta(days=2):
        return True
    
    return False


def removeInvalidFiles(folder: str):
    files = os.listdir(folder)

    manager = job.JobManager()
    for fname in files:
        fpath = os.path.join(folder, fname)

        try:
            if fileCanBeRemoved(fpath, manager):
                os.remove(fpath)
        except Exception as e:
            logging.error(e)