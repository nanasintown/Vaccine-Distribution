-- Query 1
SELECT ssNo, name, phone, role, vaccinationStatus, Employee.vaccinationPoint AS vaccinationPoint
FROM Employee, Shift, VaccinationEvent
WHERE date = '2021-05-10' AND VaccinationEvent.vaccinationPoint = Employee.vaccinationPoint
    AND Shift.employee = ssNo AND weekday = 'Monday';

-- Query 2
SELECT Employee.name AS doctor
FROM Employee, Shift, VaccinationPoint
WHERE Employee.vaccinationPoint = VaccinationPoint.name AND Shift.employee = ssNo AND weekday = 'Wednesday'
    AND role = 'doctor' AND address LIKE '%HELSINKI%';

-- Query 3
-- part 1
SELECT id, location AS currentLocation, arrivalPoint AS latestArrivalLocation
FROM Batch LEFT JOIN TransportationLog AS T ON id = batch
WHERE NOT EXISTS(
    SELECT 1
    FROM TransportationLog
    WHERE TransportationLog.batch = T.batch AND TransportationLog.arrivalDate > T.arrivalDate
)
ORDER BY id;

--part 2
SELECT id, phone
FROM Batch, VaccinationPoint, TransportationLog AS T
WHERE id = batch AND name = arrivalPoint AND NOT EXISTS(
    SELECT 1
    FROM TransportationLog
    WHERE TransportationLog.batch = T.batch AND TransportationLog.arrivalDate > T.arrivalDate
) AND location != arrivalPoint
ORDER BY id;

-- Query 4
SELECT P.ssNo, P.name, Vaccinationevent.batch, Batch.vaccine, Vaccinationappointment.date, Vaccinationappointment.vaccinationpoint 
FROM (SELECT Patient.name, Patient.ssNo
      FROM Patient, Diagnosis
      WHERE Patient.ssNo = Diagnosis.patient AND Diagnosis.date > '2021-05-10'
       AND Diagnosis.symptom IN (SELECT name FROM Symptom WHERE criticality = 't')
      GROUP BY name, ssNo) AS P, Vaccinationevent, Batch, Vaccinationappointment 
WHERE Vaccinationappointment.patient = P.ssNo AND Vaccinationevent.batch = Batch.id 
    AND Vaccinationevent.vaccinationpoint = Vaccinationappointment.vaccinationpoint AND Vaccinationevent.date = Vaccinationappointment.date;

-- Query 5
CREATE VIEW patientVaccinationStatus AS 
SELECT ssNo, name, birthday, gender, CASE WHEN ssNo IN (
    SELECT dosesTaken.patient FROM (
        SELECT patient, COUNT(*) AS doses
        FROM VaccinationAppointment
        GROUP BY patient
    ) as dosesTaken, (
        SELECT patient, requiredDoses AS doses
        FROM VaccinationAppointment AS A, VaccinationEvent AS E, Batch, Vaccine
        WHERE A.date=E.date AND A.vaccinationPoint=E.vaccinationPoint AND E.batch=Batch.ID AND Batch.vaccine=vaccine.id
            AND NOT EXISTS(SELECT 1 FROM VaccinationAppointment WHERE VaccinationAppointment.date<A.date)
    ) as dosesRequired
    WHERE dosesTaken.patient=dosesRequired.patient AND dosesTaken.doses >= dosesRequired.doses
) THEN 1 ELSE 0 END AS VaccinationStatus
FROM Patient;

-- Query 6
-- part 1
SELECT location AS "Hospital/Clinic", name AS vaccine, total AS "No. of vaccines of different types", SUM(total) OVER (PARTITION BY location) AS "No. of Vaccine" 
FROM(SELECT location, Vaccine.name, SUM(amount) AS total 
        FROM Batch JOIN Vaccine ON Vaccine.id = Batch.vaccine 
        GROUP BY location, Vaccine.name) AS tempTable;

--Query 7
WITH Tables AS(SELECT VA.patient as patientid, Vaccine.name, VA.date
    FROM VaccinationAppointment VA 
        JOIN VaccinationEvent VE ON VE.date = VA.date AND VE.vaccinationPoint = VA.vaccinationPoint
        JOIN Batch ON Batch.id = VE.batch
        JOIN Vaccine ON Vaccine.id = Batch.vaccine), 
    SymptomOccurences AS(SELECT name, symptom, COUNT(DISTINCT(Tables.patientid)) AS total 
        FROM Tables JOIN Diagnosis D ON D.patient = Tables.patientID AND D.date > Tables.date
        GROUP BY name, symptom),
    TotalVaccinations AS(SELECT name, COUNT(DISTINCT(patientid)) AS total 
        FROM Tables GROUP BY name)
SELECT SO.name AS "Vaccine", SO.symptom, 
ROUND(SO.total*1.0/TV.total, 6) AS "Frequency"
FROM SymptomOccurences AS SO JOIN TotalVaccinations AS TV ON SO.name = TV.name;