from neo4j import GraphDatabase
import pandas as pd

df = pd.read_excel('accident_data.xlsx')
neo4j_info = open('neo4j_info.txt', 'r')
url = neo4j_info.readline().strip()
ID = neo4j_info.readline().strip()
PW = neo4j_info.readline().strip()
greeter = GraphDatabase.driver(url, auth=(ID, PW))

def parse_ext_tuple(rdf_string):
    #문자열로 변환
    rdf_string = str(rdf_string)
    #괄호 균형 맞추며 튜플 추출 ((),(),()) () 안에는 ()가 또 들어갈 수 있으므로 첫 (를 찾고 대응되는 ) 괄호를 찾으면 그 사이의 문자열을 추출
    balance = []
    extracted = []
    start = 0
    for i in range(len(rdf_string)):
        if rdf_string[i] == '(':
            balance.append(i)
        elif rdf_string[i] == ')':
            start = balance.pop()
            if len(balance) == 1:
                extracted.append(rdf_string[start:i+1])
    return extracted


def add_incident(tx, Incident, pubPrivFlag, dateOccur, designSafetyRev, investMethod, futureActionPlan, safetyMgmtPlans,
                 workerNum, processRates, constrDuration, bidRate, constrCost, recurPrevMeasures, postAccMeasures,
                 reportReasons, accidentCause, accidentHistory, specificCause, incidentTime, state, district, site,
                 places, humidity, temperature, weather):
    print(f"{Incident} added")
    query = f"""
    CREATE (a:Incident {{name: '{Incident}', bidRate : '{bidRate}', constructionCost : '{constrCost}',start : '{constrDuration[0]}', end : '{constrDuration[1]}', processRates : '{processRates}', workerNum : '{workerNum}', pubprivFlag : '{pubPrivFlag}', safetyMgmtPlans : '{safetyMgmtPlans}', investMethod : '{investMethod}', designSafetyReview : '{designSafetyRev}', date : '{dateOccur}', incidentTime : '{incidentTime}', state : '{state}', district : '{district}', site : '{site}', places : '{places}', humidity : '{humidity}', temperature : '{temperature}', weather : '{weather}'}})
    MERGE (j:FutureActionPlan {{name: '{futureActionPlan}'}})
    MERGE (s:RecurrencePrevention {{name: '{recurPrevMeasures}'}})
    MERGE (t:PostAccidentMeasures {{name: '{postAccMeasures}'}})
    MERGE (w:Cause {{name: '{accidentCause}'}})
    MERGE (x:AccidentHistory {{name: '{accidentHistory}'}})
    MERGE (v:ReportReasons {{name: '{reportReasons}'}})
    MERGE (z:SpecificCause {{name: '{specificCause}'}})
    MERGE (a)-[:HAS_FUTURE_ACTION_PLAN]->(j)
    MERGE (a)-[:IMPLEMENTED_RECURRENCE_PREVENTION]->(s)
    MERGE (a)-[:TOOK_POST_ACCIDENT_MEASURES]->(t)
    MERGE (a)-[:REPORTED_FOR_REASONS]->(v)
    MERGE (a)-[:CAUSED_BY]->(w)
    MERGE (a)-[:HAS_ACCIDENT_HISTORY]->(x)
    MERGE (a)-[:HAS_SPECIFIC_CAUSE]->(z)
    """

    tx.run(query)


def add_ConstrCat(tx, incident, dateOccur, constrCat_Major, constrCat_Medium):
    print(f"ConstrCat added")
    query = f"""
    MATCH (a:Incident {{name: '{incident}', date : '{dateOccur}'}})
    MERGE(ccmajor:ConstrCat_Major {{name: '{constrCat_Major}'}})
    MERGE(ccmedium:ConstrCat_Medium {{name: '{constrCat_Medium}'}})
    MERGE (a)-[:IS_MAJOR_CAT]->(ccmajor)
    MERGE (a)-[:IS_MEDIUM_CAT]->(ccmedium)
    MERGE (b)-[:INCLUDES]->(c)
    """
    tx.run(query)


def add_IncidentObj(tx, incident, dateOccur, incidentObj_Major, incidentObj_Medium):
    print(f"IncidentObj added")
    query = f"""
    MATCH (a:Incident {{name: '{incident}', date : '{dateOccur}'}})
    MERGE(b:IncidentObj_Major {{name: '{incidentObj_Major}'}})
    MERGE(c:IncidentObj_Medium {{name: '{incidentObj_Medium}'}})
    MERGE (a)-[:IS_MAJOR_CAT]->(b)
    MERGE (a)-[:IS_MEDIUM_CAT]->(c)
    MERGE (b)-[:INCLUDES]->(c)
    """
    tx.run(query)


def add_FacilityType(tx, incident, dateOccur, facilityMajorCat, facilityMediumCat, facilitySmallCat):
    print(f"FacilityType added")
    query = f"""
    MATCH (a:Incident {{name: '{incident}', date : '{dateOccur}'}})
    MERGE(b:FacilityMajorCat {{name: '{facilityMajorCat}'}})
    MERGE(c:FacilityMediumCat {{name: '{facilityMediumCat}'}})
    MERGE(d:FacilitySmallCat {{name: '{facilitySmallCat}'}})
    MERGE (a)-[:IS_MAJOR_CAT]->(b)
    MERGE (a)-[:IS_MEDIUM_CAT]->(c)
    MERGE (a)-[:IS_SMALL_CAT]->(d)
    MERGE (b)-[:INCLUDES]->(c)
    MERGE (c)-[:INCLUDES]->(d)
    """
    tx.run(query)


def add_IncidentType(tx, incident, dateOccur, humanIncident, materialAccident):
    print(f"IncidentType added")
    query = f"""
    MATCH (a:Incident {{name: '{incident}', date : '{dateOccur}'}})
    MERGE (b:HumanIncident {{name: '{humanIncident}'}})
    MERGE (c:MaterialAccident {{name: '{materialAccident}'}})
    WITH a,b,c
    WHERE NOT (a)-[:IS_HUMAN_INCIDENT]->(b) AND NOT (a)-[:IS_MATERIAL_ACCIDENT]->(c) 
    MERGE (a)-[:IS_HUMAN_INCIDENT]->(b)
    MERGE (a)-[:IS_MATERIAL_ACCIDENT]->(c)
    """
    tx.run(query)


def add_accidentHistory(tx, accidentHistory, acccidentEnv, accidentTask, accidentType):
    print(f"AccidentHistory added")
    query = f"""
    MATCH (a:AccidentHistory {{name: '{accidentHistory}'}})
    MERGE(b:AccidentEnv {{name: '{acccidentEnv}'}})
    MERGE(c:AccidentTask {{name: '{accidentTask}'}})
    MERGE(d:AccidentType {{name: '{accidentType}'}})
    MERGE (a)-[:HAS_ACCIDENT_ENV]->(b)
    MERGE (a)-[:HAS_ACCIDENT_TASK]->(c)
    MERGE (a)-[:HAS_ACCIDENT_TYPE]->(d)
    MERGE (b)-[:same_incident]->(c)
    MERGE (c)-[:same_incident]->(d)
    """
    tx.run(query)


def add_damage(tx, incident, dateOccur, fatalities, injured, damageAmount, damageDesc):
    print(f"Damage added")
    query = f"""
    MATCH (a:Incident {{name: '{incident}', date : '{dateOccur}'}})
    MERGE(b:Fatalities {{name: 'fatalities', local: '{fatalities[0]}', foreign: '{fatalities[1]}'}})
    MERGE(c:Injured {{name: 'injured', local: '{injured[0]}', foreign: '{injured[1]}'}})
    MERGE(d:DamageAmount {{name: '{damageAmount}'}})
    MERGE(e:DamageDesc {{name: '{damageDesc}'}})
    MERGE (a)-[:HAS_FATALITIES]->(b)
    MERGE (a)-[:HAS_INJURED]->(c)
    MERGE (a)-[:HAS_DAMAGE_AMOUNT]->(d)
    MERGE (a)-[:HAS_DAMAGE_DESC]->(e)
    """
    tx.run(query)

#Incident, pubPrivFlag, dateOccur,   designSafetyRev, investMethod, futureActionPlan, safetyMgmtPlans, workerNum, processRates, constrDuration, bidRate, constrCost, recurPrevMeasures, postAccMeasures, reportReasons, accidentCause, accidentHistory
# Incident, pubPrivFlag, dateOccur,   designSafetyRev, investMethod, futureActionPlan, safetyMgmtPlans, workerNum, processRates, constrDuration, bidRate, constrCost, recurPrevMeasures, postAccMeasures, reportReasons, accidentCause, accidentHistory
with greeter.session() as session:
    for i, row in df.iterrows():
        durate = (row.constrDuration_s, row.constrDuration_e)
        session.execute_write(add_incident, row.Incident, row.pubPrivFlag, row.dateOccur, row.designSafetyRev,
                              row.investMethod, row.futureActionPlan, row.safetyMgmtPlans, row.workerNum,
                              row.processRates, durate, row.bidRate, row.constrCost, row.recurPrevMeasures,
                              row.postAccMeasures, row.reportReasons, row.accidentCause, row.accidentHistory,
                              row.specificCause, row.incidentTime, row.cityDistrict_state, row.cityDistrict_district,
                              row.Location_site, row.Location_places, row.weatherCond_humidity,
                              row.weatherCond_temperature, row.weatherCond_weather)

        session.execute_write(add_ConstrCat, row.Incident, row.dateOccur, row.constrCat_Major, row.constrCat_Medium)

        session.execute_write(add_IncidentObj, row.Incident, row.dateOccur, row.incidentObj_Major,
                              row.incidentObj_Medium)

        session.execute_write(add_FacilityType, row.Incident, row.dateOccur, row.FacilityType_Major,
                              row.FacilityType_Medium, row.FacilityType_Small)

        session.execute_write(add_IncidentType, row.Incident, row.dateOccur, row.humanIncident, row.materialAccident)

        accidentType, accidentTask, acccidentEnv = parse_ext_tuple(row.accidentHistoryCls)

        session.execute_write(add_accidentHistory, row.accidentHistory, acccidentEnv, accidentTask, accidentType)

        fatalities = (row.fatalities_local, row.fatalities_foreigner)
        injured = (row.injured_local, row.injured_foreigner)

        session.execute_write(add_damage, row.Incident, row.dateOccur, fatalities, injured, row.damageAmount,
                              row.damageDesc)

