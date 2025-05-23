AWO_TRANSACTION_SALES = '''
SELECT 
    SO2 = ?
    ,*
from PSS4W{so}.dbo.vw_awobillingdata
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_SALES_INVOICE_TSO = '''
SELECT 
    FakturATPMTSOID
    ,CustomerName
    ,CustomerAddress1
    ,CustomerAddress2
    ,CustomerAddress3
    ,CustomerAddress4
    ,CustomerAddress5
    ,AreaName
    ,RegionCode
    ,NoKTPOrTDP
    ,Email
    ,PhoneNo
    ,CustomerName2
    ,RTRW
    ,Kelurahan
    ,Kecamatan
    ,FakturATPMTSOKabupaten
    ,Provinsi
    ,Pekerjaan
    ,FakturATPMID
    ,BusinessAreaID
    ,BusinessAreaCode
    ,SalesOrganizationID
    ,SalesOrganizationCode
    ,SerialNo
    ,PoliceNo
    ,KTPNo
    ,STNKDate
    ,FakturATPMKabupaten
    ,CustomerNo
    ,BranchGoodsReceiptBPKBDate
    ,SalesTypeCode
    ,SalesTypeDescription
    ,SalesOrderNo
    ,CreatedBy
    ,LastModifiedBy
    ,CreatedTime
    ,LastModifiedTime
FROM PSS4WTSO.dbo.vw_AWOFakturATPMTSODataInitial
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_SALES_INVOICE_NONTSO = '''
SELECT 
    FakturATPMNonTSOID
    ,FirstNameOnSTNK
    ,SecondNameOnSTNK
    ,FirstAddressOnSTNK
    ,SecondAddressOnSTNK
    ,Kelurahan
    ,Kecamatan
    ,City
    ,PostalCode
    ,BBNAreaID
    ,BBNAreaCode
    ,PhoneNo
    ,Religion
    ,District
    ,BirthDate
    ,Pekerjaan
    ,Pendidikan
    ,Email
    ,RT
    ,RW
    ,Kewarganegaraan
    ,KelurahanCode
    ,KecamatanCode
    ,FakturATPMID
    ,BusinessAreaID
    ,BusinessAreaCode
    ,SalesOrganizationID
    ,SalesOrganizationCode
    ,SerialNo
    ,PoliceNo
    ,STNKDate
    ,FakturATPMKabupaten
    ,CustomerNo
    ,BranchGoodsReceiptBPKBDate
    ,SalesTypeCode
    ,SalesTypeDescription
    ,SalesOrderNo
    ,CreatedBy
    ,LastModifiedBy
    ,CreatedTime
    ,LastModifiedTime
FROM PSS4W{so}.dbo.vw_AWOFakturATPMNonTSODataInitial
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_SALES_PARTNERDETAIL = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_EquipmentPartnerDetail
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_SALES_SALESORDER = '''
SELECT 
    SO2 = ?
    ,*
FROM PSS4W{so}.dbo.vw_awosalesorderdata
WHERE COALESCE(SOLastModifiedTime, SOCreatedTime) BETWEEN ? AND ?
'''

AWO_TRANSACTION_SALES_TVC = '''
SELECT 
    *
FROM PSS4W{so}.dbo.vw_awotvcdatainitial
WHERE COALESCE(LastModifiedTime, CreatedTime) BETWEEN ? AND ?
'''