CREATE USER bioflow WITH PASSWORD 'bioflow';

CREATE DATABASE bioflow OWNER bioflow ENCODING 'UTF8';

CREATE TABLE JobInfo
(
    id character varying(128) NOT NULL,
    secid character varying(128) NOT NULL,
    run integer,
    priority integer,
    name character varying(1024),
    description character varying(1024),
    pipeline character varying(1024),
    workdir character varying(1024),
    logdir character varying(1024),
    created character varying(128),
    finished character varying(128),
    state character varying(64),
    pausedstate character varying(64),
    smid character varying(512),
    retrylimit integer,
    json character varying(10485760),
    auxinfo character varying(204800),
    CONSTRAINT jobinfo_pkey PRIMARY KEY (id)
)
WITH (OIDS=FALSE);

CREATE TABLE JobScheduleInfo
(
    id character varying(128) NOT NULL,
    graphinfo character varying(512),
    execjson character varying(10485760),
    CONSTRAINT jobscheduleinfo_pkey PRIMARY KEY (id)
)
WITH (OIDS=FALSE);

CREATE TABLE JobHistory
(
    seq serial NOT NULL,
    id character varying(128) NOT NULL,
    secid character varying(128) NOT NULL,
    run integer,
    priority integer,
    name character varying(1024),
    description character varying(1024),
    pipeline character varying(1024),
    workdir character varying(1024),
    logdir character varying(1024),
    created timestamp,
    finished timestamp,
    state character varying(64),
    pausedstate character varying(64),
    smid character varying(512),
    retrylimit integer,
    failreason character varying(256),
    jobJson character varying(10485760),
    schedJson character varying(10485760),
    auxinfo character varying(204800),
    CONSTRAINT jobhistory_pkey PRIMARY KEY (seq)
)
WITH (OIDS=FALSE);

CREATE TABLE Pipelines
(
    name character varying(256) NOT NULL,
    secid character varying(128) NOT NULL,
    state int DEFAULT 0,
    json character varying(10485760),
    CONSTRAINT pipelines_pkey PRIMARY KEY (name, secid)
)
WITH (OIDS=FALSE);

CREATE TABLE PipelineItems
(
    name character varying(256) NOT NULL,
    secid character varying(128) NOT NULL,
    state int DEFAULT 0,
    json character varying(10485760),
    CONSTRAINT pipelineitems_pkey PRIMARY KEY (name, secid)
)
WITH (OIDS=FALSE);

CREATE TABLE UserInfo
(
    id character varying(128) NOT NULL,
    name character varying(256) NOT NULL,
    groupid character varying(256) NOT NULL,
    credit int DEFAULT 0,
    jobquota int DEFAULT 0,
    taskquota int DEFAULT 0,
    auxinfo character varying(10485760),
    CONSTRAINT userinfo_pkey PRIMARY KEY (id)
)
WITH (OIDS=FALSE);

CREATE TABLE AuditRscLogs
(
    seq serial NOT NULL,
    jobid character varying(128) NOT NULL,
    stageid character varying(128) NOT NULL,
	schedtime timestamp,
	userid character varying(128) NOT NULL,
	duration float(8),
	cpu float(8),
	mem float(8),
    CONSTRAINT aduitrsclogs_pkey PRIMARY KEY (seq)
)
WITH (OIDS=FALSE);
