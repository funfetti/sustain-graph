// create unique identifiers 

create constraint on (e:EUR) ASSERT e.EURId IS UNIQUE
create constraint on (a:Asset) ASSERT a.AssetId IS UNIQUE
create constraint on (c:CF) ASSERT c.CFId IS UNIQUE
create constraint on (t:RecordType) ASSERT t.RecordTypeId is UNIQUE

// load org assets 

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/funfetti/sustain-graph/master/oa.csv' as row
MERGE (a:Asset {AssetId: row.ID, Name: row.NAME, AssetType: row.SUSTAIN_APP__ASSETTYPE__C, BusinessRegion: row.SUSTAIN_APP__BUSINESSREGION__C})
MERGE (r:Region {Name: row.SUSTAIN_APP__BUSINESSREGION__C})
MERGE (t:RecordType {RecordTypeId: row.RECORDTYPEID})
MERGE (a)-[:LOCATED_IN]->(r)
MERGE (a)-[:TYPE]->(t)

// load energy use records and pair to assets 

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/funfetti/sustain-graph/master/eur.csv' as row
MERGE (e:EUR {EURId: row.ID,
    Name: row.NAME, 
    Scope1: toFloat(row.SUSTAIN_APP__SCOPE1EMISSIONSTCO2E__C),
    Scope2L: toFloat(row.SUSTAIN_APP__SCOPE2LOCATIONBASEDEMISSIONSTCO2E__C),
    Scope2M: toFloat(row.SUSTAIN_APP__SCOPE2MARKETBASEDEMISSIONSTCO2E__C),
    Scope3: toFloat(row.SUSTAIN_APP__SCOPE_3_EMISSIONS_TCO2E__C),
    Owned: toBoolean(row.SUSTAIN_APP__OWNED_ASSET__C),
    Carbon_Inventory_Date: date(row.SUSTAIN_APP__MONTH_YEAR__C),
    EUR_Start_Date: date(row.SUSTAIN_APP__STARTDATE__C),
    EUR_End_Date: date(row.SUSTAIN_APP__ENDDATE__C)
})
MERGE (a:Asset {AssetId: row.SUSTAIN_APP__ASSET__C})
MERGE (e)-[:CONSUMPTION_OF]->(a)
MERGE (t:RecordType {RecordTypeId: row.RECORDTYPEID})
MERGE (e)-[:TYPE]->(t)

// load and chain carbon footprint records 
// to do: warning i recieved... The execution plan for this query contains the Eager operator, which forces all dependent data to be materialized in main memory before proceeding
// Using LOAD CSV with a large data set in a query where the execution plan contains the Eager operator could potentially consume a lot of memory and is likely to not perform well. See the Neo4j Manual entry on the Eager operator for more information and hints on how problems could be avoided.


LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/funfetti/sustain-graph/master/cf.csv' as row 
MERGE (c:CF {CFId: row.ID,
    Name: row.NAME,
    Stage: row.SUSTAIN_APP__STAGE__C,
    Reporting_Year: toInteger(row.SUSTAIN_APP__REPORTINGYEAR__C),
    Reporting_Date: date(row.SUSTAIN_APP__REPORTING_DATE__C)
})
SET c.Previous_Year = row.SUSTAIN_APP__PY_ANNUAL_CONSUMPTION_REPORT__C, c.Org_Asset = row.SUSTAIN_APP__ASSET__C
MERGE (t:RecordType {RecordTypeId: row.RECORDTYPEID})
MERGE (c)-[:TYPE]->(t)
WITH c AS prev WHERE prev.Previous_Year IS NOT NULL 
MATCH (p:CF {CFId: prev.Previous_Year})
MERGE (prev)-[:BEFORE]->(p)

// link CF to asset... need to figure out how to combine this with above?

MATCH (n:CF) WHERE n.Org_Asset IS NOT NULL 
MATCH (a:Asset {AssetId: n.Org_Asset})
MERGE (n)-[:SUMMARY_OF]->(a)

// load cfri

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/funfetti/sustain-graph/master/cfri.csv' as row
MATCH (c:CF {CFId: row.SUSTAIN_APP__CONSUMPTIONREPORT__C})
MATCH (e:EUR {EURId: row.SUSTAIN_APP__ENERGYCONSUMPTION__C})
MERGE (e)-[j:REPORT_ITEM_OF]->(c)
SET j.Name = row.NAME

// add referenced record types 

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/funfetti/sustain-graph/master/rt.csv' as row 
MATCH (t:RecordType {RecordTypeId: row.ID})
SET t.Name = row.NAME, t.DeveloperName = row.DEVELOPERNAME, t.Object = row.SOBJECTTYPE

//      USE CASES 
//          
// 1. mass pair EUR to CF 
// 2. campus groupings


// 3. lonely nodes 
// find empty cf
MATCH (n:CF) WHERE NOT (:EUR)-[:REPORT_ITEM_OF]-(n) 
RETURN n

// find unlinked EUR

MATCH (n:EUR) WHERE NOT (n)-[:REPORT_ITEM_OF]-(:CF) 
RETURN n

// working

// no record types
match (n) where not n:RecordType return n
