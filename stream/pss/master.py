AWO_DETAIL_ACTIVITYTYPE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.vw_activitytype
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_BILLINGTYPE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.vw_billingtype
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_CHARGETO = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.chargeto
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_CITY = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.city
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_DEALER_BUSINESSAREA = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.businessarea
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_DEALER_SALESOFFICE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.SalesOffice
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_EMPLOYEE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.employee
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_JOBTYPE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.vw_jobtype
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_PARTNERFUNCTION = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.vw_partnerfunction
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_PKBTYPE = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.vw_pkbtype
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_POSITION = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.position
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_DETAIL_REGION = '''
SELECT 
    * 
FROM PSS4WMASTER.dbo.region
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

