AWO_TRANSACTION_PKB = '''
SELECT 
    SO2 = ?
    ,*
FROM PSS4W{so}.dbo.vw_PKBAWO
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_BILLING_ALT = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_BillingService
WHERE COALESCE(synchronizedon, billingdate) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_BILLING = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_BillingServiceAWO
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_BOOKING = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_BookingAWO
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_MATERIAL = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_PKBmaterial
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_OPERATION = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_PKBoperation
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_ORDERREQUEST = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_ServiceOrderRequest
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_PARTNERDETAIL = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_PKBPartnerDetail
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_PKB_SALESORDER = '''
SELECT 
    SO = ?
    ,*
FROM PSS4W{so}.dbo.vw_SalesOrderService
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''