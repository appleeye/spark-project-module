sed -i.bak '/site_id,appnexus_id,es_id,device_type,professional_group,seniority,functional_area,intent_topics,visit_total_time,visit_total_actions,last_visit_timestamp,engagement_score,country,state,city,company_name,domain,company_vertical,company_employee,company_revenue,business_tags,client_segment,segment_ids,segment_names,generate_date/d' visitor_details.csv
mysqlimport  --bind-address=192.168.20.247 -u root -p everstring_cn_123 -c site_id,content,count,generate_date ads business_tag_summary.csv
mysqlimport  --bind-address=192.168.20.247 -u root -p admin123 -c content,count,generate_date,site_id ads intent_topic_summary.csv
mysqlimport  --bind-address=192.168.20.247 -u root -p admin123 -c site_id,appnexus_id,es_id,device_type,professional_group,seniority,functional_area,intent_topics,visit_total_time,visit_total_actions,last_visit_timestamp,engagement_score,country,state,city,company_name,domain,company_vertical,company_employee,company_revenue,business_tags,client_segment,segment_ids,segment_names,generate_date
 ads visitor_details.csv


