from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from dateutil import relativedelta
import numpy as np

import matplotlib.pyplot as plt
def main():
    try:
        pd.options.display.max_columns = 500
        pd.options.display.width = 0
        DIALECT = "postgresql+psycopg2://"
        database = "grp16_vaccinedist"
        user = "grp16"
        host = "dbcourse2022.cs.aalto.fi"
        password = input("Enter password:")
        uri = "%s:%s@%s/%s" % (user, password, host, database)
        engine = create_engine(DIALECT + uri)
        conn = engine.connect()

        # Task 1
        PatientSymptoms_df = pd.read_sql_query(""" SELECT ssNo AS ssNO, gender, birthday AS dateOfBirth, symptom, 
                                                          date AS diagnosisDate
                                                   FROM Patient, Diagnosis 
                                                   WHERE patient = ssNo;""", conn)
        PatientSymptoms_df.to_sql("PatientSymptoms", conn, index=True, if_exists="replace")
        print("Task 1")
        print(PatientSymptoms_df)
        print("------------------------------------------------------------------------------------------------------")

        # Task 2
        conn.execute(""" CREATE TEMPORARY TABLE PatientAppointment AS  
                         SELECT ssNo, VA.date, Vaccine.name AS vaccinetype 
                         FROM Patient, VaccinationAppointment AS VA, VaccinationEvent AS VE, Batch, Vaccine
                         WHERE ssNo = patient AND VA.vaccinationPoint = VE.vaccinationPoint AND VA.date = VE.date 
                         AND VE.batch = Batch.id  AND Batch.vaccine = Vaccine.id;""")
        conn.execute(""" CREATE TEMPORARY TABLE FirstAppointment AS  
                         SELECT *
                         FROM PatientAppointment AS PA
                         WHERE NOT EXISTS(
                            SELECT * 
                            FROM PatientAppointment
                            WHERE PatientAppointment.ssNo = PA.ssNo AND PatientAppointment.date < PA.date);""")
        conn.execute(""" CREATE TEMPORARY TABLE NotFirstAppointment AS
                         SELECT * 
                         FROM PatientAppointment
                         EXCEPT 
                         SELECT *
                         FROM FirstAppointment;""")
        conn.execute(""" CREATE TEMPORARY TABLE SecondAppointment AS  
                         SELECT *
                         FROM NotFirstAppointment AS NFA
                         WHERE NOT EXISTS(
                            SELECT * 
                            FROM NotFirstAppointment
                            WHERE NotFirstAppointment.ssNo = NFA.ssNo AND NotFirstAppointment.date < NFA.date);""")
        PatientVaccineInfo_df = pd.read_sql_query(
            """ SELECT Patient.ssNo AS patientssNO, FirstAppointment.date AS date1, FirstAppointment.vaccinetype AS vaccinetype1,
                       SecondAppointment.date AS date2, SecondAppointment.vaccinetype AS vaccinetype2
                FROM Patient LEFT JOIN FirstAppointment ON Patient.ssNO = FirstAppointment.ssNo 
                             LEFT JOIN SecondAppointment ON FirstAppointment.ssNo = SecondAppointment.ssNo;""", conn)
        PatientVaccineInfo_df.to_sql("PatientVaccineInfo", conn, index=True, if_exists="replace")
        print("Task 2")
        print(PatientVaccineInfo_df)
        print("------------------------------------------------------------------------------------------------------")

        # Task 3
        PatientSymptoms_df = pd.read_sql_query(""" SELECT *
                                                   FROM "PatientSymptoms";""", conn)
        MaleSymptoms = PatientSymptoms_df[PatientSymptoms_df["gender"] == "M"]
        FemaleSymptoms = PatientSymptoms_df[PatientSymptoms_df["gender"] == "F"]
        MaleSymptomsTop3 = MaleSymptoms.groupby("symptom").agg({"ssno": "count"}).sort_values("ssno", ascending=False).head(3)
        FemaleSymptomsTop3 = FemaleSymptoms.groupby("symptom").agg({"ssno": "count"}).sort_values("ssno", ascending=False).head(3)
        MaleSymptomsTop3.rename(columns={"ssno": "count"}, inplace=True)
        FemaleSymptomsTop3.rename(columns={"ssno": "count"}, inplace=True)
        print("Task 3")
        print("Female Symptoms")
        print(FemaleSymptoms)
        print("Top 3 Female Symptoms")
        print(FemaleSymptomsTop3)
        print("Male Symptoms")
        print(MaleSymptoms)
        print("Top 3 Male Symptoms")
        print(MaleSymptomsTop3)
        print("------------------------------------------------------------------------------------------------------")

        # Task 4
        Patient_df = pd.read_sql_query(""" SELECT * FROM patient;""", conn)
        AgeColumn = Patient_df["birthday"].apply(lambda b: relativedelta.relativedelta(datetime.now(), datetime.strptime(b, "%Y-%m-%d")).years)
        Patient_df["ageGroup"] = pd.cut(AgeColumn, [0, 10, 20, 40, 60, 200], labels=["0-9", "10-19", "20-39", "40-59", "60+"], right=False)
        print("Task 4")
        print(Patient_df)
        print("------------------------------------------------------------------------------------------------------")

        # Task 5
        VaccineStatus = PatientVaccineInfo_df[["patientssno", "date1", "date2"]].apply(
            lambda r: pd.Series([r[0], (1 if r[1] else 0) + (1 if r[2] else 0)], ["ssno", "vaccinationStatus"]), axis=1)
        PatientVaccineStatus = pd.merge(Patient_df, VaccineStatus, on="ssno")
        print("Task 5")
        print(PatientVaccineStatus)
        print("------------------------------------------------------------------------------------------------------")

        # Task 6
        percentages = PatientVaccineStatus.groupby(["ageGroup", "vaccinationStatus"]).aggregate({"ssno": "count"}).reset_index()
        percentages["ssno"] = percentages["ssno"] / percentages.groupby("ageGroup")["ssno"].transform("sum") * 100
        percentages.rename(columns={"ssno": "percent"}, inplace=True)
        percentages["percent"] = percentages["percent"].apply(lambda v: "{:.2f}".format(v) + "%" if v == v else "-")
        percentages = percentages.pivot(index="vaccinationStatus", columns="ageGroup", values="percent")
        print("Task 6")
        print(percentages)
        print("------------------------------------------------------------------------------------------------------")

        # Task 7
        ##########
        frequency = pd.read_sql_query("""WITH Tables AS(SELECT VA.patient as patientid, Vaccine.id, VA.date
                                        FROM VaccinationAppointment VA
                                        JOIN VaccinationEvent VE ON VE.date = VA.date AND VE.vaccinationPoint = VA.vaccinationPoint
                                        JOIN Batch ON Batch.id = VE.batch
                                        JOIN Vaccine ON Vaccine.id = Batch.vaccine
                                        ),
                                        SymptomOccurences AS(SELECT id, symptom, COUNT(DISTINCT(Tables.patientid)) AS total
                                        FROM Tables JOIN Diagnosis D ON D.patient = Tables.patientID AND D.date > Tables.date
                                        GROUP BY id, symptom),
                                        TotalVaccinations AS(SELECT id, COUNT(DISTINCT(patientid)) AS total
                                        FROM Tables GROUP BY id)
                                        SELECT SO.id, SO.symptom,
                                        ROUND(SO.total*1.0/TV.total, 6) AS frequency
                                        FROM SymptomOccurences AS SO JOIN TotalVaccinations AS TV ON SO.id = TV.id;""", conn)
        frequency = frequency.pivot(index="symptom", columns="id", values="frequency")
        frequency = frequency.fillna(0)
        for c in ["V01", "V02", "V03"]:
            frequency[c] = pd.cut(frequency[c], [-0.0001, 0.0, 0.05, 0.1, 1], labels=["-", "rare", "common", "very common"])

        print("Task 7")
        print(frequency)
        print("------------------------------------------------------------------------------------------------------")

        # Task 8
        attendance_percentages = pd.read_sql("""SELECT count::FLOAT/amount*100 AS percentage
                                                FROM Batch, VaccinationEvent AS VE, (
                                                    SELECT Date, VaccinationPoint, COUNT(patient)
                                                    FROM VaccinationAppointment
                                                    GROUP BY (Date, VaccinationPoint)
                                                ) AS C
                                                WHERE VE.date=C.date AND VE.VaccinationPoint=C.VaccinationPoint AND VE.Batch=Batch.id""", conn)
        optimal_percentage = np.mean(attendance_percentages) + np.std(attendance_percentages)
        print("Task 8")
        print(f"There should be {optimal_percentage[0]}% of vaccines reserved")
        print("------------------------------------------------------------------------------------------------------")
        
        # Task 9: 
        print("Task 9")
        PatientVaccineInfoDF = pd.read_sql("""SELECT * FROM "PatientVaccineInfo"; """, engine)
        
        firstDoseTotals = np.cumsum( PatientVaccineInfoDF['date1'].value_counts().sort_index() )
        firstDoseTotals.index = firstDoseTotals.index.map(lambda d: datetime.strptime(d,'%Y-%m-%d').date())
        secondDoseTotals = np.cumsum( PatientVaccineInfoDF['date2'].value_counts().sort_index() )
        secondDoseTotals.index = secondDoseTotals.index.map(lambda d: datetime.strptime(d,'%Y-%m-%d').date())

        firstDoseTotals.plot()
        secondDoseTotals.plot()
        plt.title('Vaccinations by date')
        plt.xlabel('Date')
        plt.ylabel('Total Vaccinated Patients by Date')
        plt.xticks(rotation = 45)
        plt.legend(["First dose", "Second dose"])
        plt.show()
        # Task 10
        print("Task 10")
        ssNOAndNamedf = pd.read_sql_query("""SELECT patient AS ssNO, name AS Name FROM Vaccinationappointment INNER JOIN 
                                          Patient ON Vaccinationappointment.patient = Patient.ssno 
                                          WHERE vaccinationpoint IN (SELECT vaccinationpoint FROM Employee 
                                          WHERE ssno = '19740919-7140') AND date <= '2021-05-15' AND date >= '2021-05-05' 
                                          UNION SELECT ssno, name FROM Employee WHERE vaccinationpoint 
                                          IN (SELECT vaccinationpoint FROM Employee WHERE ssno = '19740919-7140') AND ssno != '19740919-7140'""")
        print(ssNOAndNamedf)
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()
            
    
    


main()
