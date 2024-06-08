from dataclasses import dataclass, asdict
import db

@dataclass
class VideoCompressionStatistics:
    vcj: str
    originalSizeBytes: int
    finalSizeBytes: int
    startTimestamp: int
    endTimestamp: int


@dataclass
class VideoCompressionStatisticsReport:
    averageReductionRate: float
    maxReductionRate: float
    minReductionRate: float
    allTimeSavedBytes: int
    largestVideoCompressedSize: int
    smallestVideoCompressedSize: int
    minCompressionTime: float
    maxCompressionTime: float
    averageCompressionTime: float
    minCompressedBytesPerSecond: float
    averageCompressedBytesPerSecond: float
    maxCompressedBytesPerSecond: float

def saveVideoCompressionStatistics(statistics: VideoCompressionStatistics):
    dbInstance = db.getDbInstance()

    dbInstance.runUpdateQuery("INSERT INTO VideoCompressionStatistics (vcj, originalSizeBytes, finalSizeBytes, startTimestamp, endTimestamp) VALUES (?,?,?,?,?)", [
        statistics.vcj,
        statistics.originalSizeBytes,
        statistics.finalSizeBytes,
        statistics.startTimestamp,
        statistics.endTimestamp
    ])


def generateVideoCompressionStatisticsDict() -> dict:
    return asdict(generateVideoCompressionStatistics())

def generateVideoCompressionStatistics() -> VideoCompressionStatisticsReport:
    dbInstance = db.getDbInstance()

    averageReductionRate = 0
    maxReductionRate = 0
    minReductionRate = 0
    allTimeSavedBytes = 0
    largestVideoCompressedSize = 0
    smallestVideoCompressedSize = 0
    minCompressionTime = 0
    maxCompressionTime = 0
    averageCompressionTime = 0
    minCompressedBytesPerSecond = 0
    averageCompressedBytesPerSecond = 0
    maxCompressedBytesPerSecond = 0
  
    result = dbInstance.runGetQuery("""

        SELECT
        AVG(1.0 * originalSizeBytes / finalSizeBytes),                   
        MAX(1.0 * originalSizeBytes / finalSizeBytes),
        MIN(1.0 * originalSizeBytes / finalSizeBytes),
        SUM(originalSizeBytes - finalSizeBytes),
        MAX(originalSizeBytes),
        MIN(originalSizeBytes),
        MIN(endTimestamp - startTimestamp),
        MAX(endTimestamp - startTimestamp),
        AVG(endTimestamp - startTimestamp),
        MIN(1.0 * finalSizeBytes / (endTimestamp - startTimestamp)),
        AVG(1.0 * finalSizeBytes / (endTimestamp - startTimestamp)),
        MAX(1.0 * finalSizeBytes / (endTimestamp - startTimestamp))
        FROM VideoCompressionStatistics
    """)

    if len(result) == 1:
        row = result[0]
        averageReductionRate = row[0]
        maxReductionRate = row[1]
        minReductionRate = row[2]
        allTimeSavedBytes = row[3]
        largestVideoCompressedSize = row[4]
        smallestVideoCompressedSize = row[5]
        minCompressionTime = row[6]
        maxCompressionTime = row[7]
        averageCompressionTime = row[8]
        minCompressedBytesPerSecond = row[9]
        averageCompressedBytesPerSecond = row[10]
        maxCompressedBytesPerSecond = row[11]

    report = VideoCompressionStatisticsReport(
        averageReductionRate,
        maxReductionRate,
        minReductionRate,
        allTimeSavedBytes,
        largestVideoCompressedSize,
        smallestVideoCompressedSize,
        minCompressionTime,
        maxCompressionTime,
        averageCompressionTime,
        minCompressedBytesPerSecond,
        averageCompressedBytesPerSecond,
        maxCompressedBytesPerSecond
    )
    
    return report
    
