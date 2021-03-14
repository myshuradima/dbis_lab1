# -*- coding: utf8 -*-
cteate_table = """CREATE TABLE IF NOT EXISTS odata1(
OUTID VARCHAR(255) PRIMARY KEY,
Birth VARCHAR(255),
SEXTYPENAME VARCHAR(255),
REGNAME VARCHAR(255),
AREANAME VARCHAR(255),
TERNAME VARCHAR(255),
REGTYPENAME VARCHAR(255),
TerTypeName VARCHAR(255),
ClassProfileNAME VARCHAR(255),
ClassLangName VARCHAR(255),
EONAME VARCHAR(265),
EOTYPENAME VARCHAR(255),
EORegName VARCHAR(255),
EOAreaName VARCHAR(255),
EOTerName VARCHAR(255),
EOParent VARCHAR(255),
UkrTest VARCHAR(255),
UkrTestStatus VARCHAR(255),
UkrBall100 INTEGER,
UkrBall12 INTEGER,
UkrBall INTEGER,
UkrAdaptScale VARCHAR(255),
UkrPTName VARCHAR(255),
UkrPTRegName VARCHAR(255),
UkrPTAreaName VARCHAR(255),
UkrPTTerName VARCHAR(255),
histTest VARCHAR(255),
HistLang VARCHAR(255),
histTestStatus VARCHAR(255),
histBall100 INTEGER,
histBall12 INTEGER,
histBall INTEGER,
histPTName VARCHAR(255),
histPTRegName VARCHAR(255),
histPTAreaName VARCHAR(255),
histPTTerName VARCHAR(255),
mathTest VARCHAR(255),
mathLang VARCHAR(255),
mathTestStatus VARCHAR(255),
mathBall100 INTEGER,
mathBall12 INTEGER,
mathBall INTEGER,
mathPTName VARCHAR(255),
mathPTRegName VARCHAR(255),
mathPTAreaName VARCHAR(255),
mathPTTerName VARCHAR(255),
physTest VARCHAR(255),
physLang VARCHAR(255),
physTestStatus VARCHAR(255),
physBall100 INTEGER,
physBall12 INTEGER,
physBall INTEGER,
physPTName VARCHAR(255),
physPTRegName VARCHAR(255),
physPTAreaName VARCHAR(255),
physPTTerName VARCHAR(255),
chemTest VARCHAR(255),
chemLang VARCHAR(255),
chemTestStatus VARCHAR(255),
chemBall100 INTEGER,
chemBall12 INTEGER,
chemBall INTEGER,
chemPTName VARCHAR(255),
chemPTRegName VARCHAR(255),
chemPTAreaName VARCHAR(255),
chemPTTerName VARCHAR(255),
bioTest VARCHAR(255),
bioLang VARCHAR(255),
bioTestStatus VARCHAR(255),
bioBall100 INTEGER,
bioBall12 INTEGER,
bioBall INTEGER,
bioPTName VARCHAR(255),
bioPTRegName VARCHAR(255),
bioPTAreaName VARCHAR(255),
bioPTTerName VARCHAR(255),
geoTest VARCHAR(255),
geoLang VARCHAR(255),
geoTestStatus VARCHAR(255),
geoBall100 INTEGER,
geoBall12 INTEGER,
geoBall INTEGER,
geoPTName VARCHAR(255),
geoPTRegName VARCHAR(255),
geoPTAreaName VARCHAR(255),
geoPTTerName VARCHAR(255),
engTest VARCHAR(255),
engTestStatus VARCHAR(255),
engBall100 INTEGER,
engBall12 INTEGER,
engDPALevel VARCHAR(255),
engBall INTEGER,
engPTName VARCHAR(255),
engPTRegName VARCHAR(255),
engPTAreaName VARCHAR(255),
engPTTerName VARCHAR(255),
fraTest VARCHAR(255),
fraTestStatus VARCHAR(255),
fraBall100 INTEGER,
fraBall12 INTEGER,
fraDPALevel VARCHAR(255),
fraBall INTEGER,
fraPTName VARCHAR(255),
fraPTRegName VARCHAR(255),
fraPTAreaName VARCHAR(255),
fraPTTerName VARCHAR(255),
deuTest VARCHAR(255),
deuTestStatus VARCHAR(255),
deuBall100 INTEGER,
deuBall12 INTEGER,
deuDPALevel VARCHAR(255),
deuBall INTEGER,
deuPTName VARCHAR(255),
deuPTRegName VARCHAR(255),
deuPTAreaName VARCHAR(255),
deuPTTerName VARCHAR(255),
spaTest VARCHAR(255),
spaTestStatus VARCHAR(255),
spaBall100 INTEGER,
spaBall12 INTEGER,
spaDPALevel VARCHAR(255),
spaBall INTEGER,
spaPTName VARCHAR(255),
spaPTRegName VARCHAR(255),
spaPTAreaName VARCHAR(255),
spaPTTerName VARCHAR(255),
examdate VARCHAR(255));"""

ball_list = [18, 19, 20, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]

insert_string = """INSERT INTO odata1(
OUTID
,Birth
,SEXTYPENAME
,REGNAME
,AREANAME
,TERNAME
,REGTYPENAME
,TerTypeName
,ClassProfileNAME
,ClassLangName
,EONAME
,EOTYPENAME
,EORegName
,EOAreaName
,EOTerName
,EOParent
,UkrTest
,UkrTestStatus
,UkrBall100
,UkrBall12
,UkrBall
,UkrAdaptScale
,UkrPTName
,UkrPTRegName
,UkrPTAreaName
,UkrPTTerName
,histTest
,HistLang
,histTestStatus
,histBall100
,histBall12
,histBall
,histPTName
,histPTRegName
,histPTAreaName
,histPTTerName
,mathTest
,mathLang
,mathTestStatus
,mathBall100
,mathBall12
,mathBall
,mathPTName
,mathPTRegName
,mathPTAreaName
,mathPTTerName
,physTest
,physLang
,physTestStatus
,physBall100
,physBall12
,physBall
,physPTName
,physPTRegName
,physPTAreaName
,physPTTerName
,chemTest
,chemLang
,chemTestStatus
,chemBall100
,chemBall12
,chemBall
,chemPTName
,chemPTRegName
,chemPTAreaName
,chemPTTerName
,bioTest
,bioLang
,bioTestStatus
,bioBall100
,bioBall12
,bioBall
,bioPTName
,bioPTRegName
,bioPTAreaName
,bioPTTerName
,geoTest
,geoLang
,geoTestStatus
,geoBall100
,geoBall12
,geoBall
,geoPTName
,geoPTRegName
,geoPTAreaName
,geoPTTerName
,engTest
,engTestStatus
,engBall100
,engBall12
,engDPALevel
,engBall
,engPTName
,engPTRegName
,engPTAreaName
,engPTTerName
,fraTest
,fraTestStatus
,fraBall100
,fraBall12
,fraDPALevel
,fraBall
,fraPTName
,fraPTRegName
,fraPTAreaName
,fraPTTerName
,deuTest
,deuTestStatus
,deuBall100
,deuBall12
,deuDPALevel
,deuBall
,deuPTName
,deuPTRegName
,deuPTAreaName
,deuPTTerName
,spaTest
,spaTestStatus
,spaBall100
,spaBall12
,spaDPALevel
,spaBall
,spaPTName
,spaPTRegName
,spaPTAreaName
,spaPTTerName
,examdate
) VALUES(
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s);
"""
select_query = """SELECT regname,
	avg(physball100),
	avg(physball12),
	examdate
FROM public.odata
WHERE odata.physteststatus = 'Зараховано'
GROUP BY regname, examdate;"""