from sqlalchemy import create_engine
import pandas as pd

VERBOSE = False


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

        sheets = pd.read_excel("../data/vaccine-distribution-data-cleaned.xlsx", sheet_name=None)

        for sheet in sheets:
            sheets[sheet] = sheets[sheet].rename(columns=str.strip)

        vaccine_type_df = sheets["VaccineType"]
        try:
            vaccine_type_df["doses"] = vaccine_type_df["doses"].astype(int)
            vaccine_type_df["tempMin"] = vaccine_type_df["tempMin"].astype(int)
            vaccine_type_df["tempMax"] = vaccine_type_df["tempMax"].astype(int)
        except:
            print("Major Error in data: Invalid value(s) for doses/tempMin/tempMax for vaccine(s)")
            exit(-1)
        sheets["VaccineType"] = vaccine_type_df

        sheets["VaccineBatch"]["amount"] = sheets["VaccineBatch"]["amount"].astype(int)
        sheets["VaccineBatch"]["manufDate"] = sheets["VaccineBatch"]["manufDate"].astype(str).apply(lambda date: date.split(" ")[0])
        sheets["VaccineBatch"]["expiration"] = sheets["VaccineBatch"]["expiration"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["Transportation log"]["dateArr"] = sheets["Transportation log"]["dateArr"].astype(str).apply(lambda date: date.split(" ")[0])
        sheets["Transportation log"]["dateDep"] = sheets["Transportation log"]["dateDep"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["StaffMembers"]["vaccination status"] = sheets["StaffMembers"]["vaccination status"].astype(bool)
        sheets["StaffMembers"]["date of birth"] = sheets["StaffMembers"]["date of birth"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["Vaccinations"]["date"] = sheets["Vaccinations"]["date"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["Patients"]["date of birth"] = sheets["Patients"]["date of birth"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["VaccinePatients"]["date"] = sheets["VaccinePatients"]["date"].astype(str).apply(lambda date: date.split(" ")[0])

        sheets["Symptoms"]["criticality"] = sheets["Symptoms"]["criticality"].astype(bool)

        sheets["Diagnosis"]["date"] = sheets["Diagnosis"]["date"].astype(str).apply(lambda date: date.split(" ")[0])

        Manufacturer = sheets["Manufacturer"][["ID", "phone"]].rename(columns={"ID": "id"}).rename(columns=str.lower)
        ProductionFacility = sheets["Manufacturer"][["ID", "country"]].rename(columns={"ID": "manufacturer"}) \
            .rename(columns=str.lower)
        Vaccine = sheets["VaccineType"][["ID", "name", "doses", "tempMin", "tempMax"]] \
            .rename(columns={"ID": "id", "doses": "requiredDoses"}).rename(columns=str.lower)
        License = sheets["Manufacturer"][["ID", "vaccine"]].rename(columns={"ID": "manufacturer"}).rename(columns=str.lower)
        VaccinationPoint = sheets["VaccinationStations"].rename(columns=str.lower)
        Batch = sheets["VaccineBatch"][["batchID", "type", "amount", "manufacturer", "manufDate", "expiration", "location"]] \
            .rename(columns={"batchID": "ID", "type": "vaccine", "manufDate": "productionDate", "expiration": "expiryDate"}) \
            .rename(columns=str.lower)
        TransportationLog = sheets["Transportation log"][["batchID", "dateDep", "departure", "dateArr", "arrival"]] \
            .rename(columns={"batchID": "batch", "dateDep": "departureDate", "departure": "departurePoint", "dateArr": "arrivalDate", "arrival": "arrivalPoint"}) \
            .rename(columns=str.lower)
        Employee = sheets["StaffMembers"] \
            .rename(columns={"social security number": "ssNo", "date of birth": "birthday", "vaccination status": "vaccinationStatus", "hospital": "vaccinationPoint"}) \
            .rename(columns=str.lower)
        Shift = sheets["Shifts"][["weekday", "worker"]].rename(columns={"worker": "employee"}) \
            .rename(columns=str.lower)
        VaccinationEvent = sheets["Vaccinations"].rename(columns={"location": "vaccinationPoint", "batchID": "batch"}) \
            .rename(columns=str.lower)
        Patient = sheets["Patients"].rename(columns={"date of birth": "birthday"}).rename(columns=str.lower)
        VaccinationAppointment = sheets["VaccinePatients"][["patientSsNo", "date", "location"]] \
            .rename(columns={"patientSsNo": "patient", "location": "vaccinationPoint"}).rename(columns=str.lower)
        Symptom = sheets["Symptoms"].rename(columns=str.lower)
        Diagnosis = sheets["Diagnosis"].rename(columns=str.lower)

        Manufacturer.to_sql("manufacturer", conn, if_exists='append', index=False)
        ProductionFacility.to_sql("productionfacility", conn, if_exists='append', index=False)
        Vaccine.to_sql("vaccine", conn, if_exists='append', index=False)
        License.to_sql("license", conn, if_exists='append', index=False)
        VaccinationPoint.to_sql("vaccinationpoint", conn, if_exists='append', index=False)
        Batch.to_sql("batch", conn, if_exists='append', index=False)
        TransportationLog.to_sql("transportationlog", conn, if_exists='append', index=False)
        Employee.to_sql("employee", conn, if_exists='append', index=False)
        Shift.to_sql("shift", conn, if_exists='append', index=False)
        VaccinationEvent.to_sql("vaccinationevent", conn, if_exists='append', index=False)
        Patient.to_sql("patient", conn, if_exists='append', index=False)
        VaccinationAppointment.to_sql("vaccinationappointment", conn, if_exists='append', index=False)
        Symptom.to_sql("symptom", conn, if_exists='append', index=False)
        Diagnosis.to_sql("diagnosis", conn, if_exists='append', index=False)

        if VERBOSE:
            pd.set_option('display.max_columns', None)
            print(Manufacturer)
            print("----------------------------------------------------------------")
            print(ProductionFacility)
            print("----------------------------------------------------------------")
            print(Vaccine)
            print("----------------------------------------------------------------")
            print(License)
            print("----------------------------------------------------------------")
            print(VaccinationPoint)
            print("----------------------------------------------------------------")
            print(Batch)
            print("----------------------------------------------------------------")
            print(TransportationLog)
            print("----------------------------------------------------------------")
            print(Employee)
            print("----------------------------------------------------------------")
            print(Shift)
            print("----------------------------------------------------------------")
            print(VaccinationEvent)
            print("----------------------------------------------------------------")
            print(Patient)
            print("----------------------------------------------------------------")
            print(VaccinationAppointment)
            print("----------------------------------------------------------------")
            print(Symptom)
            print("----------------------------------------------------------------")
            print(Diagnosis)
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()


main()
