CREATE TABLE IF NOT EXISTS jobs (
    uuid UUID PRIMARY KEY,
    state TEXT CHECK(state IN ('PENDING', 'COMPLETED', 'FAILED')),
    type TEXT CHECK(type IN ('VIDEO_COMPRESSION_JOB')),
    createdAt TIMESTAMP NOT NULL,
    expiresAt TIMESTAMP
);

CREATE TABLE IF NOT EXISTS VideoCompressionJob (
    job UUID,
    originalFilePath TEXT NOT NULL,
    destinationFilePath TEXT NOT NULL,
    quality TEXT NOT NULL,
    factor INT NOT NULL,
    framerate INT NOT NULL,
    FOREIGN KEY (job) REFERENCES jobs(id)
);

CREATE TABLE IF NOT EXISTS VideoCompressionStatistics (
    vcj UUID,
    originalSizeBytes INT NOT NULL,
    finalSizeBytes INT NOT NULL,
    startTimestamp INT NOT NULL,
    endTimestamp INT NOT NULL,
    FOREIGN KEY (vcj) REFERENCES VideoCompressionJob(job)
);

CREATE TABLE IF NOT EXISTS Client (
    apikey UUID NOT NULL,
    revoked BOOLEAN NOT NULL
);