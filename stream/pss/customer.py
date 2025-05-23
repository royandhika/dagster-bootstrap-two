AWO_CUSTOMER = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomerdata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_ADDRESS = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomeraddressdata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_ADDRESSUSAGE = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomeraddressusagedata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_EMAIL = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomeremaildata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_FAX = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomerfaxdata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_FLAT = '''
SELECT 
    c.CustomerNo
    ,CFD.NPWP
    ,CFD.KTPNo
    ,CFD.PassportNo
    ,CFD.Kelurahan
    ,CFD.Kecamatan
    ,CFD.Religion
    ,CFD.Birthday
    ,CFD.CreatedTime
    ,CFD.CreatedBy
    ,CFD.LastModifiedTime
    ,CFD.LastModifiedBy
    ,CFD.IsActive
    ,CFD.KITASNo
    ,CFD.SIMNo
    ,CFD.BirthPlace
    ,CFD.RT
    ,CFD.RW
    ,CFD.Provinsi
    ,CFD.StatusPerkawinan
    ,CFD.Pekerjaan
    ,CFD.Kewarganegaraan
    ,CFD.GolonganDarah
    ,CFD.Hobby
    ,CFD.FavoriteDrinks
    ,CFD.SpecialNotes
    ,CFD.AlamatKTP
    ,Ct.CityCode
    ,Ct.CityDescription
    ,CFD.IsOCR
    ,CFD.JenisKelamin
    ,CFD.KartuKeluarga
    ,CFD.NITKU 
FROM PSS4WMASTER.dbo.customerflatdata CFD
INNER JOIN PSS4WMASTER.dbo.Customer C 
    ON C.ID = CFD.CustomerID 
INNER JOIN PSS4WMASTER.dbo.City Ct 
    ON Ct.ID = CFD.CityID
WHERE C.IsProspect = 0
    AND COALESCE(CFD.LastModifiedTime, CFD.CreatedTime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_MARKETINGATTRIBUTE = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomermarketingattributeinitialdata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''

AWO_CUSTOMER_TELEPHONE = '''
SELECT 
    *
FROM PSS4WMASTER.dbo.vw_awocustomertelephonedata
WHERE COALESCE(lastmodifiedtime, createdtime) BETWEEN ? AND ?
'''