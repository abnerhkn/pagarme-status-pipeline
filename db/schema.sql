DROP TABLE IF EXISTS incidentes CASCADE;

CREATE TABLE incidentes (
    incident_id VARCHAR(50) PRIMARY KEY,  
    title       TEXT NOT NULL,
    summary     TEXT,
    published   TIMESTAMP NOT NULL,
    updated     TIMESTAMP NOT NULL,
    status      VARCHAR(50) NOT NULL
);
