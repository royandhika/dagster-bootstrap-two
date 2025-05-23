{{
    config(
        materialized='incremental',
        schema=env_var('ENV_SCHEMA') + '_di',
        unique_key=['customerno'],
        group='pss_awo_customer',
        tags=['daily']
    )
}}

with customer as (
    select 
        *
        ,rn = row_number() over(partition by customerno order by coalesce(lastmodifiedtime, createdtime) desc)
    from {{ source(env_var('ENV_SCHEMA') + '_dl', 'awo_customer_flat_pss_staging') }}
    {% if is_incremental() %}
    where coalesce(lastmodifiedtime, createdtime) >= '{{ var('min_date') }}'
        and coalesce(lastmodifiedtime, createdtime) <= '{{ var('max_date') }}'
    {% endif %}
)
,final as (
    select 
        customerno
        ,ktp = case when ktpno = '' then null else ktpno end
        ,npwp = case when npwp = '' then null else npwp end
        ,passport = case when passportno = '' then null else passportno end
        ,kk = case when kartukeluarga = '' then null else kartukeluarga end
        ,nitku = case when nitku = '' then null else nitku end
        ,kitas = case when kitasno = '' then null else kitasno end
        ,sim = case when simno = '' then null else simno end
        ,address = case when alamatktp = '' then null else upper(alamatktp) end
        ,rt = case when rt = '' then null else rt end
        ,rw = case when rw = '' then null else rw end
        ,subdistrict = case when kelurahan = '' then null else upper(kelurahan) end
        ,district = case when kecamatan = '' then null else upper(kecamatan) end
        -- ,citycode
        ,city = case when citydescription = '' then null else upper(citydescription) end
        ,region = case when provinsi = '' then null else upper(provinsi) end
        ,nationality = case when kewarganegaraan = '' then null else upper(kewarganegaraan) end
        ,gender = case 
            when jeniskelamin = '' then null 
            when jeniskelamin like 'laki%' then 'MALE'
            when jeniskelamin like 'perempuan' then 'FEMALE'
            else jeniskelamin 
        end
        ,birthday = case when birthday = '' then null else convert(date, birthday) end
        ,birthplace = case when birthplace = '' then null else upper(birthplace) end
        ,religion = case when religion = '' then null else upper(religion) end
        ,statusperkawinan = case 
            when statusperkawinan = '' then null 
            when statusperkawinan like 'KAWIN%' then 'MARRIED'
            when statusperkawinan like 'BELUM%' then 'SINGLE'
            when statusperkawinan like 'CERAI%' then 'DIVORCED' 
            else statusperkawinan 
        end
        ,pekerjaan = case when pekerjaan = '' then null else upper(pekerjaan) end
        ,golongandarah = case when golongandarah = '' then null else upper(golongandarah) end
        ,hobby = case when hobby = '' then null else upper(hobby) end
        ,favoritedrinks = case when favoritedrinks = '' then null else upper(favoritedrinks) end
        ,specialnotes = case when specialnotes = '' then null else upper(specialnotes) end
        ,isocr
        ,isactive
        ,createdtime
        ,createdby
        ,lastmodifiedtime
        ,lastmodifiedby
        ,uploaddate = getdate()
    from customer 
    where rn = 1
)
select * from final