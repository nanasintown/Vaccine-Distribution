CREATE TABLE Manufacturer(
    id VARCHAR PRIMARY KEY,
    phone varchar NOT NULL
);

CREATE TABLE ProductionFacility(
    manufacturer VARCHAR REFERENCES Manufacturer(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    country VARCHAR,
    PRIMARY KEY(manufacturer, country)
);

CREATE TABLE Vaccine(
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    requiredDoses INTEGER NOT NULL,
    tempMin INTEGER NOT NULL,
    tempMax INTEGER NOT NULL
);

CREATE TABLE License(
    manufacturer VARCHAR REFERENCES Manufacturer(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    vaccine VARCHAR REFERENCES Vaccine(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    PRIMARY KEY(manufacturer, vaccine)
);

CREATE TABLE VaccinationPoint(
    name VARCHAR PRIMARY KEY,
    address VARCHAR NOT NULL,
    phone VARCHAR NOT NULL
);

CREATE TABLE Batch(
    id VARCHAR PRIMARY KEY,
    vaccine VARCHAR NOT NULL REFERENCES Vaccine(id) ON UPDATE CASCADE,
    amount INTEGER NOT NULL,
    manufacturer VARCHAR NOT NULL REFERENCES Manufacturer(id) ON UPDATE CASCADE,
    productionDate VARCHAR NOT NULL,
    expiryDate VARCHAR NOT NULL,
    location VARCHAR REFERENCES VaccinationPoint(name)
);

CREATE TABLE TransportationLog(
    batch VARCHAR REFERENCES Batch(id),
    departureDate VARCHAR NOT NULL,
    departurePoint VARCHAR NOT NULL REFERENCES VaccinationPoint(name) ON UPDATE CASCADE,
    arrivalDate VARCHAR,
    arrivalPoint VARCHAR REFERENCES VaccinationPoint(name) ON UPDATE CASCADE,
    PRIMARY KEY(batch, arrivalDate, arrivalPoint)
);

CREATE TABLE Employee(
    ssNo VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    birthday VARCHAR NOT NULL,
    phone VARCHAR NOT NULL,
    role VARCHAR NOT NULL CHECK(role IN ('doctor', 'nurse')),
    vaccinationStatus BOOLEAN NOT NULL,
    vaccinationPoint VARCHAR NOT NULL REFERENCES VaccinationPoint(name)
);

CREATE DOMAIN WeekdayDomain VARCHAR CHECK(VALUE IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'));

CREATE TABLE Shift(
    weekday WeekdayDomain,
    employee VARCHAR REFERENCES Employee(ssNo) ON DELETE CASCADE,
    PRIMARY KEY(weekday,employee)
);

CREATE TABLE VaccinationEvent(
    date VARCHAR,
    vaccinationPoint VARCHAR REFERENCES VaccinationPoint(name)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    batch VARCHAR NOT NULL REFERENCES Batch(id),
    PRIMARY KEY(date, vaccinationPoint)
);

CREATE TABLE Patient(
    ssNo VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    birthday VARCHAR NOT NULL,
    gender CHAR(1) NOT NULL CHECK(gender IN ('F', 'M', 'O'))
);

CREATE TABLE VaccinationAppointment(
    patient VARCHAR REFERENCES Patient(ssNo) ON DELETE CASCADE,
    date VARCHAR NOT NULL,
    vaccinationPoint VARCHAR NOT NULL,
    PRIMARY KEY(patient, date, vaccinationPoint),
    FOREIGN KEY(date, vaccinationPoint) REFERENCES VaccinationEvent(date, vaccinationPoint)
);

CREATE TABLE Symptom(
    name VARCHAR PRIMARY KEY,
    criticality BOOLEAN NOT NULL
);

CREATE TABLE Diagnosis(
    patient VARCHAR REFERENCES Patient(ssNo) ON DELETE CASCADE,
    symptom VARCHAR REFERENCES Symptom(name),
    date VARCHAR,
    PRIMARY KEY(patient, symptom, date)
);
