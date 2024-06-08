import db
import uuid
import db
import abc
from enum import Enum
from dataclasses import dataclass
import logging
from datetime import datetime, timedelta
import threading
import os
import job_statistics
import ffmpeg

class JobState(Enum):
    PENDING = "PENDING",
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class JobType(Enum):
    VIDEO_COMPRESSION_JOB = "VIDEO_COMPRESSION_JOB"

@dataclass
class BaseJobData:
    uuid: str
    state: JobState
    type: JobType
    createdAt: datetime
    expiresAt: datetime | None

@dataclass
class VideoCompressorJobData:
    baseJobData: BaseJobData
    originalFilePath: str
    destinationFilePath: str
    quality: str
    factor: int
    framerate: str


class Job:
    def __init__(self, data: BaseJobData):
        self.baseData = data


    def isExpired(self):
        if self.baseData.expiresAt == None:
            return False
        
        if datetime.now() > self.baseData.expiresAt:
            return True
        
        return False

    def setExpiresAt(self, expiresAt: datetime):
        self.baseData.expiresAt = expiresAt

    def setCompleted(self):
        self.baseData.state = JobState.COMPLETED

    def setFailed(self):
        self.baseData.state = JobState.FAILED

    def save(self):
        dbInstance = db.getDbInstance()

        result = dbInstance.runGetQuery("SELECT * FROM jobs WHERE uuid = ?", [self.baseData.uuid])

        if len(result) == 0:
            dbInstance.runUpdateQuery("INSERT INTO jobs (uuid, state, type, createdAt, expiresAt) VALUES (?,?,?,?,?)", [
                self.baseData.uuid,
                self.baseData.state.name,
                self.baseData.type.name,
                self.baseData.createdAt,
                self.baseData.expiresAt
            ]) 

            return

        dbInstance.runUpdateQuery("UPDATE jobs SET state=?,createdAt=?,expiresAt=? WHERE uuid=?", [
            self.baseData.state.name,
            self.baseData.createdAt,
            self.baseData.expiresAt,
            self.baseData.uuid
        ])
        

    @abc.abstractmethod
    def run(self):
        pass

    def toDict(self):
        return {
            "uuid" : self.baseData.uuid,
            "expiresAt": self.baseData.expiresAt.isoformat() if self.baseData.expiresAt is not None else None,
            "createdAt": self.baseData.createdAt.isoformat(),
            "state": self.baseData.state.name,
            
        } 
class VideoCompressionJob(Job):
    def __init__(self, videoData: VideoCompressorJobData):
        super().__init__(videoData.baseJobData)
        
        self.originalFilePath = videoData.originalFilePath
        self.destinationFilePath = videoData.destinationFilePath
        self.framerate = videoData.framerate
        self.factor = videoData.factor
        self.quality = videoData.quality
    

    def run(self):
        logging.info("Running compression ...")

        config = ffmpeg.CompressVideoConfig(
            self.destinationFilePath,
            self.originalFilePath,
            self.factor,
            self.framerate,
            self.quality
        )
        
        start = datetime.now()
        ffmpeg.compressVideo(config)
        end = datetime.now()
        originalSize = os.stat(self.originalFilePath).st_size
        finalSize = os.stat(self.destinationFilePath).st_size

        startTimestamp = int(start.timestamp())
        endTimestamp = int(end.timestamp())

        if startTimestamp == endTimestamp:
            endTimestamp += 1
        try:
            job_statistics.saveVideoCompressionStatistics(
                job_statistics.VideoCompressionStatistics(
                    self.baseData.uuid,
                    originalSize,
                    finalSize,
                    startTimestamp=startTimestamp,
                    endTimestamp=endTimestamp
                )
            )
        except Exception as e:
            logging.error(e)

    def save(self):
        super().save()
        
        dbInstance = db.getDbInstance()

        result = dbInstance.runGetQuery("SELECT * FROM VideoCompressionJob WHERE job=?", [self.baseData.uuid])

        if len(result) == 0:
            dbInstance.runUpdateQuery("INSERT INTO VideoCompressionJob (job, originalFilePath, destinationFilePath,framerate,factor,quality) VALUES (?,?,?,?,?,?)", [
                self.baseData.uuid,
                self.originalFilePath,
                self.destinationFilePath,
                self.framerate,
                self.factor,
                self.quality
            ])

            dbInstance.runUpdateQuery("UPDATE jobs SET state = ? WHERE uuid = ?", [self.baseData.state.name, self.baseData.uuid])

            return
        
        dbInstance.runUpdateQuery("UPDATE VideoCompressionJob SET originalFilePath=?, destinationFilePath=?,framerate=?,factor=?,quality=? WHERE job = ?", [
            self.originalFilePath,
            self.destinationFilePath,
            self.framerate,
            self.factor,
            self.quality,
            self.baseData.uuid
        ])

        

    def toDict(self):
        baseDict = super().toDict()
        baseDict["originalFilePath"] = self.originalFilePath
        baseDict["destinationFilePath"] = self.destinationFilePath
        baseDict["framerate"] = self.framerate
        baseDict["quality"] = self.quality
        baseDict["factor"] = self.factor
        return baseDict

    



class JobManager:
    
    def __init__(self):
        self.jobs: list[Job] = []
        self.activeJob = None
        self.emptyJobCondition = threading.Condition()


    def runBlocking(self):

        self.recoverStateFromDatabase()

        logging.info("Job Manager started working ...")
        
        while True: 
            job = self.getNextJob()   
            self.activeJob = job
            try:
                job.run()
                job.setCompleted()
                job.setExpiresAt(datetime.now() + timedelta(days=2))
            except Exception as e:
                logging.error(f"Job failed -> {str(e)}")
                job.setFailed()
                job.setExpiresAt(datetime.now() + timedelta(days=2))

            finally:
                job.save()
                self.activeJob = None

    def getActiveJobList(self):
        return list(map(lambda x: x.toDict(), [self.activeJob] + self.jobs if self.activeJob else self.jobs))

    def getNextJob(self):
        with self.emptyJobCondition:
            while len(self.jobs) == 0:
                logging.info("No jobs available, waiting for jobs ...")
                self.emptyJobCondition.wait()

            job = self.jobs.pop(0)
            return job 

    def pushJob(self, job: Job, save=True):
        with self.emptyJobCondition:
            if save:
                job.save()

            self.jobs.append(job)
            logging.info("Job added: %s", job)
            self.emptyJobCondition.notify()


    def getJobById(self, uuid: str):
        dbInstance = db.getDbInstance()

        result = dbInstance.runGetQuery("SELECT uuid,type,state,createdAt,expiresAt FROM jobs WHERE uuid = ?", [uuid])

        if len(result) != 1:
            return None
        
        value = result[0]

        uuid = value[0]
        type = JobType[value[1]]
        state = JobState[value[2]] 
        createdAt = datetime.fromisoformat(value[3]) if value[3] else None  
        expiresAt = datetime.fromisoformat(value[4]) if value[4] else None

        if type not in [JobType.VIDEO_COMPRESSION_JOB]:
            logging.warning(f"Cannot recover job of type '{type}'")
            return None
        

        if type == JobType.VIDEO_COMPRESSION_JOB:
                element = dbInstance.runGetQuery("SELECT job,originalFilePath,destinationFilePath,framerate,factor,quality FROM VideoCompressionJob WHERE job = ?", [uuid])
                
                if len(element) != 1:
                    logging.warning(f"Cannot recover job of type '{JobType.VIDEO_COMPRESSION_JOB.value}', improper state!")
                    return None

                element = element[0]

                originalFilePath = element[1]
                destinationFilePath = element[2]
                framerate = element[3]
                factor = element[4]
                quality = element[5]

                vcjob = VideoCompressionJob(
                    VideoCompressorJobData(
                        BaseJobData(uuid, state, type, createdAt, expiresAt),
                        originalFilePath,
                        destinationFilePath,
                        quality,
                        factor,
                        framerate
                    )
                )

                return vcjob
        

    def recoverStateFromDatabase(self):
        dbInstance = db.getDbInstance()
        
        result = dbInstance.runGetQuery("SELECT uuid,type,state,createdAt,expiresAt FROM jobs WHERE state = 'PENDING'")
        toBeRecovered = []
        for row in result:
            uuid = row[0]
            type = JobType[row[1]]
            state = JobState[row[2]] 
            createdAt = datetime.fromisoformat(row[3]) if row[3] else None  
            expiresAt = datetime.fromisoformat(row[4]) if row[4] else None

            if type not in [JobType.VIDEO_COMPRESSION_JOB]:
                logging.warning(f"Cannot recover job of type '{type}'")
                continue

            if type == JobType.VIDEO_COMPRESSION_JOB:
                element = dbInstance.runGetQuery("SELECT job,originalFilePath,destinationFilePath,framerate,factor,quality FROM VideoCompressionJob WHERE job = ?", [uuid])

                if len(element) != 1:
                    logging.warning(f"Cannot recover job of type '{JobType.VIDEO_COMPRESSION_JOB.value}', improper state!")

                element = element[0]

                originalFilePath = element[1]
                destinationFilePath = element[2]
                framerate = element[3]
                factor = element[4]
                quality = element[5]

                vcjob = VideoCompressionJob(
                    VideoCompressorJobData(
                        BaseJobData(uuid, state, type, createdAt, expiresAt),
                        originalFilePath,
                        destinationFilePath,
                        quality,
                        factor,
                        framerate
                    )
                )

                toBeRecovered.append(vcjob)

        for job in toBeRecovered:
            self.pushJob(job, save=False)

                
    def getRelatedJobs(self, fname: str):
        dbInstance = db.getDbInstance()

        query = dbInstance.runGetQuery("SELECT job FROM VideoCompressionJob WHERE originalFilePath=? OR destinationFilePath=?", [fname, fname])
        
        jobs = []
        
        for row in query:
            jobs.append(self.getJobById(row[0]))

        return jobs
        


_instance = None
def getJobManager() -> JobManager:
    global _instance
    if _instance is None:
        _instance = JobManager()

    return _instance

