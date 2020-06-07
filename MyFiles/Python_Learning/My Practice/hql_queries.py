# example queries, will be different across different db platform
record_count_source_bd_person = ('''
    select  'bd_person_Source' as tableName, count(*)
    from    (
                select  *,
                        row_number () over (partition by s.indv_party_id order by s.process_timestamp desc) as rn
                from    inv_jhi_typed_qa.sdh_individual
            ) a  
    where   a.rn = 1;
''')

record_count_target_bd_person = ('''
    select  'bd_person_Target' as tableName, count(*) 
    from    inv_curated_eod_qa.bd_person 
    where   src = 'JHISDH';
''')


record_count_source_bd_entity = ('''
    select  'bd_entity_Source' as tableName, count(*)
    from    (
                select  *,
                        row_number () over (partition by s.firm_party_id order by s.process_timestamp desc) as rn
                from    inv_jhi_typed_qa.sdh_firm
            ) a  
    where   a.rn = 1;
''')

record_count_target_bd_entity = ('''
    select  'bd_entity_Target' as tableName, count(*) 
    from    inv_curated_eod_qa.bd_entity 
    where   src = 'JHISDH';
''')


# exporting queries
class HqlQuery:
  def __init__(self, source_query, target_query):
    self.source_query = source_query
    self.target_query = target_query
    
# create instances for SqlQuery 
bd_person_query = HqlQuery(record_count_source_bd_person, record_count_target_bd_person)
bd_entity_query = HqlQuery(record_count_source_bd_entity, record_count_target_bd_entity)

# store as list for iteration
rc_querys = [bd_person_query, bd_entity_query]