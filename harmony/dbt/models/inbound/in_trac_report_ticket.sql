SELECT 
a.ticket_id,
a.categorization_time AS created_time,
a.sap_code,
(SELECT group_code FROM cc_master_sap WHERE id=a.sap_code LIMIT 1)sap_groupcode,

e.name lov1,

(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_1' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov2,
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_2' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov3, 
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_3' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov4,
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_4' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov5,
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_5' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov6,
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_category xx ON 1x1.data_content=xx.id WHERE 1x1.field_name_id='category_ticket_6' AND 1x1.ticket_id=a.ticket_id LIMIT 1) lov7,

(SELECT sla_release FROM cc_master_category WHERE id = a.task_category LIMIT 1) sla_release,
(SELECT sla_complete FROM cc_master_category WHERE id = a.task_category LIMIT 1) sla_complete,
(SELECT sla_close FROM cc_master_category WHERE id = a.task_category LIMIT 1) sla_close,
(SELECT NAME FROM cc_user WHERE id= a.pickup_by LIMIT 1)agentCreate,
(SELECT xz.equipment_id FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)equipment_id,
(SELECT xz.equipment_category FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)equipment_category,

(SELECT xz.brand FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)brand, 
(SELECT xz.model FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)model,
(SELECT xz.series FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)series,
(SELECT xz.segment FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)segment,
(SELECT xz.year FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)YEAR,
(SELECT xz.cylinder FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)cylinder,
(SELECT xz.transmission FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)transmission,
(SELECT xz.color FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)color,
(SELECT xz.equipment_description FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)equipment_description,

DATE_FORMAT(
(SELECT xz.stnk_date FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)
,'%Y-%m-%d') AS stnk_date,
(SELECT xz.insurance_number FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)insurance_number,
DATE_FORMAT(
(SELECT xz.req_start_date FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)
,'%Y-%m-%d %H:%i:%s') AS RentalStartDate,
DATE_FORMAT(
(SELECT xz.req_end_date FROM cc_master_equipment xz WHERE zz.data_content=xz.equipment_id LIMIT 1)
,'%Y-%m-%d %H:%i:%s') AS RentalEndDate,

(SELECT 1x1.data_content FROM cc_queue_detail 1x1  WHERE 1x1.field_name_id='txtReporterEmail' AND 1x1.ticket_id=a.ticket_id LIMIT 1) AS Email, 
(SELECT 1x2.data_content FROM cc_queue_detail 1x2  WHERE 1x2.field_name_id='txtReporterName' AND 1x2.ticket_id=a.ticket_id LIMIT 1) AS CP1,
(SELECT 1x3.data_content FROM cc_queue_detail 1x3  WHERE 1x3.field_name_id='txtReporterPhone' AND 1x3.ticket_id=a.ticket_id LIMIT 1) AS Phone_CP1,
(SELECT 1x4.data_content FROM cc_queue_detail 1x4  WHERE 1x4.field_name_id='txtContactName1' AND 1x4.ticket_id=a.ticket_id LIMIT 1) AS CP2,
(SELECT 1x5.data_content FROM cc_queue_detail 1x5  WHERE 1x5.field_name_id='txtContactPhone1' AND 1x5.ticket_id=a.ticket_id LIMIT 1) AS Phone_CP2,
(SELECT 1x6.data_content FROM cc_queue_detail 1x6  WHERE 1x6.field_name_id='txtIncidentTitle' AND 1x6.ticket_id=a.ticket_id LIMIT 1) AS IncidentTitle,
(SELECT 1x7.data_content FROM cc_queue_detail 1x7  WHERE 1x7.field_name_id='txtCause' AND 1x7.ticket_id=a.ticket_id LIMIT 1) AS CauseText,
(SELECT 1x8.data_content FROM cc_queue_detail 1x8  WHERE 1x8.field_name_id='txtPoAccu' AND 1x8.ticket_id=a.ticket_id LIMIT 1) AS PoAccu,
REPLACE(REPLACE(
	(SELECT 1x9.data_content FROM cc_queue_detail 1x9  WHERE 1x9.field_name_id='notes' AND 1x9.ticket_id=a.ticket_id LIMIT 1)
	,'\r', ''), '\n', '')AS GeneralNotes,
(SELECT 1x10.data_content FROM cc_queue_detail 1x10  WHERE 1x10.field_name_id='txtOdometer' AND 1x10.ticket_id=a.ticket_id LIMIT 1) AS Odometer,
a.customer_id,
(SELECT zxx.bussiness_type_desc FROM cc_master_customer_general zxx WHERE zxx.customer_id = a.customer_id LIMIT 1)AS BussinessTypeName,
(SELECT zxx.name1 FROM cc_master_customer_general zxx WHERE zxx.customer_id = a.customer_id LIMIT 1)AS CustomerName1,
(SELECT zxx.name2 FROM cc_master_customer_general zxx WHERE zxx.customer_id = a.customer_id LIMIT 1)AS CustomerName2,
a.source_id,
(SELECT b.name FROM cc_ticket_time_frame c JOIN cc_user b ON c.pic=b.id WHERE c.ticket_id=a.ticket_id ORDER BY c.response_time DESC LIMIT 1) AS AgentComplete, 
a.close_ticket_time AS ResultDate, 
(SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="H" ORDER BY response_time ASC LIMIT 1)AS DateProgress,
(SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="V" ORDER BY response_time ASC LIMIT 1)AS DateComplete,
(SELECT xcc.to_eskalasi FROM cc_ticket_escalation xcc WHERE xcc.ticket_id = a.ticket_id AND xcc.level=99 AND xcc.type_eskalasi='task_create' LIMIT 1)AS EmailCreateTo,
(SELECT xcc.cc_eskalasi FROM cc_ticket_escalation xcc WHERE xcc.ticket_id = a.ticket_id AND xcc.level=99 AND xcc.type_eskalasi='task_create' LIMIT 1)AS EmailCreateCC, 
(SELECT xx.name FROM cc_queue_detail 1x1 JOIN cc_master_cabang xx ON xx.id=1x1.data_content WHERE 1x1.field_name_id='optCabang' AND 1x1.ticket_id=a.ticket_id LIMIT 1)AS CabangName,
(SELECT xx.code FROM cc_queue_detail 1x1 JOIN cc_master_cabang xx ON xx.id=1x1.data_content WHERE 1x1.field_name_id='optCabang' AND 1x1.ticket_id=a.ticket_id LIMIT 1)AS CabangCode,
REPLACE(REPLACE(aaba.title_note,'\r', ''), '\n', '') AS	TaskTitleNotes,
REPLACE(REPLACE(
	(SELECT GROUP_CONCAT(description,'\n',response_time) FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND task_id=aaba.task_id AND group_id="AGENT_CB")
	,'\r', ''), '\n', '') AS TaskGeneralNotes,
v.code AS STATUS, 
DATE_FORMAT(a.created_time,'%Y-%m-%d %H:%i:%s') AS NotifCreate,
DATE_FORMAT(
	(SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="V" ORDER BY task_id DESC,response_time ASC LIMIT 1)
	,'%Y-%m-%d %H:%i:%s') AS TaskRelease,
DATE_FORMAT(
	(SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="V" ORDER BY task_id DESC,response_time ASC LIMIT 1)
	,'%Y-%m-%d %H:%i:%s') AS TaskComplete,
DATE_FORMAT(
	(SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="C" ORDER BY task_id DESC,response_time ASC LIMIT 1)
	,'%Y-%m-%d %H:%i:%s') AS NotifComplete,
DATE_FORMAT(a.categorization_time,'%Y-%m-%d %H:%i:%s') AS DateCreated,
DATE_FORMAT((SELECT DATE_ADD(aaba.target_start, INTERVAL a.sla_group HOUR)),'%Y-%m-%d %H:%i:%s') AS  TargetRelease,
DATE_FORMAT((SELECT DATE_ADD((SELECT DATE_ADD(aaba.target_start, INTERVAL a.sla_group HOUR)), INTERVAL a.sla_backdesk HOUR)),'%Y-%m-%d %H:%i:%s') AS TargetComplete,
DATE_FORMAT((SELECT response_time FROM cc_ticket_time_frame WHERE ticket_id=a.ticket_id AND current_status="C" ORDER BY task_id DESC,response_time ASC LIMIT 1),'%Y-%m-%d %H:%i:%s') AS TargetClose,
(SELECT GROUP_CONCAT(attachment) FROM cc_ticket_time_frame ax WHERE ax.ticket_id=a.ticket_id AND (ax.attachment !="" OR ax.attachment IS NOT NULL) GROUP BY ax.ticket_id) AS attachment, 
a.layanan AS survey, a.desc_layanan AS Alasan

FROM cc_queue a 
JOIN `cc_master_reference` `b` ON `a`.`source_id`=`b`.`ref` AND `b`.`id`="CHANNEL_MODULE" 
JOIN `cc_master_category` `e` ON `a`.`first_category`=`e`.`id` 
JOIN `cc_master_reference` `v` ON `a`.`queue_status`=`v`.`ref` AND `v`.`id`="TICKET_STATUS" 
JOIN `cc_user` `z` ON `a`.`pic_group_section`=`z`.`id`
LEFT JOIN cc_ticket_task aaba ON aaba.ticket_id = a.ticket_id
LEFT JOIN cc_queue_detail zz ON a.ticket_id=zz.ticket_id AND zz.field_name_id='equipment_id' AND zz.module='ticket'

WHERE DATE(a.categorization_time) > ?
AND a.is_ticket = "2" 
AND a.first_category  != "0"

-- ORDER BY `a`.`info_lastupdate_time` DESC




with cc_queue as (
    select 
        a.ticket_id 
        ,created_time = a.categorization_time 
        ,a.sap_code 
        ,sap_groupcode = b.group_code 
        -- ,first_category 
        ,lov1 = c.name 
        ,a.source_id
        ,f.sla_release
        ,f.sla_complete
        ,f.sla_close
        ,f.sla_close
        ,agentcreate = g.name
        ,agentcomplete = g.name
        ,tasktitlenotes = replace(replace(i.title_note, '\r', ''), '\n', '')
    from cc_queue a
    left join cc_master_sap b 
        on a.sap_code = b.id
    left join cc_master_category c 
        on a.first_category = c.id 
    left join cc_master_reference d 
        on a.queue_status = d.ref 
        and d.id = 'TICKET_STATUS'
    left join cc_master_reference e 
        on a.source_id = e.ref 
        and e.id = 'CHANNEL_MODULE'
    left join cc_master_category f 
        on a.task_category = f.id
    left join cc_user g 
        on a.pickup_by = g.id 
    left join cc_user h 
        on a.last_response_by = g.id 
    left join cc_ticket_task i 
        on a.ticket_id = i.ticket_id 
)
,cc_queue_detail as (
    select
        a.ticket_id
        ,lov2 = max(case when a.field_name_id = 'category_ticket_1' then c.name end) 
        ,lov3 = max(case when a.field_name_id = 'category_ticket_2' then c.name end) 
        ,lov4 = max(case when a.field_name_id = 'category_ticket_3' then c.name end) 
        ,lov5 = max(case when a.field_name_id = 'category_ticket_4' then c.name end) 
        ,lov6 = max(case when a.field_name_id = 'category_ticket_5' then c.name end) 
        ,lov7 = max(case when a.field_name_id = 'category_ticket_6' then c.name end) 
        ,email = max(case when a.field_name_id = 'txtReporterEmail' then a.data_content end) 
        ,cp1 = max(case when a.field_name_id = 'txtReporterName' then a.data_content end) 
        ,phone_cp1 = max(case when a.field_name_id = 'txtReporterPhone' then a.data_content end) 
        ,cp2 = max(case when a.field_name_id = 'txtContactName1' then a.data_content end) 
        ,phone_cp2 = max(case when a.field_name_id = 'txtContactPhone1' then a.data_content end) 
        ,incidenttitle = max(case when a.field_name_id = 'txtIncidentTitle' then a.data_content end) 
        ,cabangname = max(case when a.field_name_id = 'optCabang' then c.name end) 
        ,cabangcode = max(case when a.field_name_id = 'optCabang' then c.code end) 
    from cc_queue_detail a
    left join cc_master_category c 
        on a.data_content = c.id 
    where exists (
        select 1
        from cc_queue b
        where a.ticket_id = b.ticket_id
    )
)
,

select 
    a.ticket_id
    ,created_time = a.categorization_time 
    ,a.sap_code 
    ,sap_groupcode = b.group_code
    ,case 
        when 



    ,lov1 = e.name
    ,lov2 = d1.name
    ,lov3 = d2.name
    ,lov4 = d3.name
    ,lov5 = d4.name
    ,lov6 = d5.name
    ,lov7 = d6.name
    ,d.sla_release
    ,d.sla_complete
    ,d.sla_close 
    ,d.sla_close 
    ,e.name 

from cc_queue a 
left join cc_master_sap b 
    on a.sap_code = b.id 
left join cc_queue_detail c
    on a.ticket_id = c.ticket_id 
left join cc_master_category d 
    on c.data_content = d.id 
left join cc_user e 
    on a.pickup_by = e.id 
left join cc_master_equipment f 
    on c.data_content = f.equipment_id
    and c.field_name_id = 'equipmentno_id'
left join cc_master_customer_general g 
    on a.customer_id = g.customer_id 
left join cc_ticket_time_frame h 
    on 
left join cc_master_category i 
    on a


    -- and c1.field_name_id = 'category_ticket_1'
left join cc_queue_detail c2
    on a.ticket_id = c2.ticket_id 
    and c2.field_name_id = 'category_ticket_2'
left join cc_queue_detail c3
    on a.ticket_id = c3.ticket_id 
    and c3.field_name_id = 'category_ticket_3'
left join cc_queue_detail c4
    on a.ticket_id = c4.ticket_id 
    and c4.field_name_id = 'category_ticket_4'
left join cc_queue_detail c5
    on a.ticket_id = c5.ticket_id 
    and c5.field_name_id = 'category_ticket_5'
left join cc_queue_detail c6
    on a.ticket_id = c6.ticket_id 
    and c6.field_name_id = 'category_ticket_6'
left join cc_queue_detail cc
    on a.ticket_id = cc.ticket_id 


left join cc_master_category d1
    on c1.data_content = d1.id
left join cc_master_category d2
    on c2.data_content = d2.id
left join cc_master_category d3
    on c3.data_content = d3.id
left join cc_master_category d4
    on c4.data_content = d4.id
left join cc_master_category d5
    on c5.data_content = d5.id
left join cc_master_category d6
    on c6.data_content = d6.id
left join cc_master_category d 
    on a.task_category = d.id 
left join cc_user e 
    on a.pickup_by = e.id
left join cc_master_equipment f 
    on a.
