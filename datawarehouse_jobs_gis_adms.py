# %%
import pandas as pd, sqlalchemy as sql, pyodbc, sqlite3, datetime as dt, cx_Oracle as ora, time

start_execution_time = time.time()
ora.init_oracle_client(lib_dir=r"C:/Oracle_64/product/11.2.0/client_1/BIN/")

engine_adms = sql.create_engine("mssql+pyodbc://EpsaReportes:cmXoasys2@10.238.109.61\OASYSHDB:20010/ADMS_QueryEngine?driver=SQL Server")
engine_oracle = sql.create_engine("oracle+cx_oracle://RDJARAMILLO:Berlin22+@PV10262/arcgis")

# %%
print("Inicia lectura bases de datos ADMS")
sub_execution_time = time.time()
extract_info = pd.read_sql_query("""SELECT ID
      ,EXTRACT_ALIAS
      ,EXTRACT_STATE_ID
      ,EXTRACT_SOURCE_ID
      ,DESCRIPTION
      ,CREATED_DATE
      ,LAST_MODIFIED_DATE
      ,FILENAME
      ,COMMENT
  FROM CSRepo.dbo.EXTRACT_INFO""", engine_adms, coerce_float=False, dtype='str')

extract_state = pd.read_sql_query("""SELECT ID
      ,STATE_NAME
  FROM CSRepo.dbo.EXTRACT_STATE""", engine_adms, coerce_float=False, dtype='str')

extract_source = pd.read_sql_query("""SELECT ID
      ,SOURCE_NAME
  FROM CSRepo.dbo.EXTRACT_SOURCE""", engine_adms, coerce_float=False, dtype='str')

extract_hist = pd.read_sql_query("""SELECT EXTRACT_ID
      , USERNAME
      , TRANSITION_TIME
  FROM CSRepo.dbo.EXTRACT_HISTORY""", engine_adms, coerce_float=False, dtype='str')

changeset_extract_dependency = pd.read_sql_query("""SELECT CHANGESET_ID
      ,EXTRACT_ID
  FROM CSRepo.dbo.CHANGESET_EXTRACT_DEPENDENCY""", engine_adms, coerce_float=False, dtype='str')

changeset_info = pd.read_sql_query("""SELECT CHANGESET_STATE_ID
      , CHANGESET_TYPE_ID
      , DESCRIPTION
      , ID
      , LAST_MODIFIED_DATE
  FROM CSRepo.dbo.CHANGESET_INFO""", engine_adms, coerce_float=False, dtype='str')

changeset_state = pd.read_sql_query("""SELECT STATE_NAME
      , ID FROM CSRepo.dbo.CHANGESET_STATE""",engine_adms, coerce_float=False, dtype='str')

changeset_hist = pd.read_sql_query("""SELECT CHANGESET_ID
      , OLD_CHANGESET_STATE_ID
      , NEW_CHANGESET_STATE_ID
      , USERNAME
      , TRANSITION_TIME
  FROM CSRepo.dbo.CHANGESET_HISTORY""", engine_adms, coerce_float=False, dtype='str')

changeset_type = pd.read_sql_query("SELECT ID, TYPE_NAME  FROM CSRepo.dbo.CHANGESET_TYPE",engine_adms, coerce_float=False, dtype='str')
print("Finaliza lectura bases de datos ADMS en {}".format(time.time() - sub_execution_time))

print("Inicia lectura tabla GIS")
sub_execution_time = time.time()
gis_jobs = pd.read_sql_query("""SELECT JJSX.job_id, jj.JOB_NAME, jjs.STEP_NAME, v.owner || '.' || v.name, jj.NOTES , jj.DESCRIPTION , jj.OWNED_BY 
    FROM GRED_ADMINIS.JTX_STEP_STATUS jjsx 
    LEFT JOIN GRED_ADMINIS.JTX_JOB_STEP jjs ON jjs.STEP_ID  = JJSX.step_id
    LEFT JOIN GRED_ADMINIS.JTX_JOBS jj ON jj.JOB_ID = jjsx.JOB_ID 
    LEFT JOIN sde.versions v ON v.name LIKE '%'|| jj.JOB_NAME ||'%'
    WHERE 1 = 1
    AND jjs.STEP_NAME IN ('Espera Confirmación ADMS','Confirmar al ADMS','Promover en ADMS', ' Rechazar en ADMS','Finalizado')
    AND jjsx.STATUS = 'S'
    """, engine_oracle)

print("Finaliza lectura tabla GIS en {}".format(time.time() - sub_execution_time))

# %%
print("Inicia lectura de archivo NE")
sub_execution_time = time.time()

NE_Invoker1_file = pd.read_csv(r'\\10.240.160.176\f$\Network Exporter Invoker\Log\NetworkExporterExecutionService.log', sep='\r',header = None)
NE_Invoker1_file['Machine'] = 'PV10219'
NE_Invoker2_file = pd.read_csv(r'\\10.240.160.161\e$\Network Exporter Invoker\Log\NetworkExporterExecutionService.log', sep='\r',header = None)
NE_Invoker2_file['Machine'] = 'PV10270'
NE_Invoker = pd.concat([NE_Invoker1_file,NE_Invoker2_file ])
NE_Invoker_Pending =  NE_Invoker[NE_Invoker[0].str.contains('ColaMensajes.Encolar')].reset_index(drop=True)
NE_Invoker_Executed =  NE_Invoker[NE_Invoker[0].str.contains('Se esta realizando la siguiente llamada')].reset_index(drop=True)

print("Finaliza lectura archivos NE en {}".format(time.time() - sub_execution_time))

# %%
NE_Invoker_Pending['datetime'] = NE_Invoker_Pending[0].str.split('.',expand=True)[0]
NE_Invoker_Pending[['Version','KindExecution','FeederList']] = NE_Invoker_Pending[0].str.split('Version:|kindExportation:|listFeeders:',expand=True)[[1,2,3]]
NE_Invoker_Pending.KindExecution = NE_Invoker_Pending.KindExecution.str.split(',',expand = True)[0]
NE_Invoker_Pending.Version = NE_Invoker_Pending.Version.str[:-2]
NE_Invoker_Pending.datetime = NE_Invoker_Pending.datetime.replace({'a':'AM','p':'PM'},regex=True)
NE_Invoker_Pending.datetime = list(map(lambda x: dt.datetime.strptime(x,'%d/%m/%Y %I:%M:%S %p'), NE_Invoker_Pending.datetime))
NE_Invoker_Pending = NE_Invoker_Pending.drop(columns = 0).sort_values(by='datetime').reset_index(drop=True)
NE_Invoker_Pending['CustomId'] = list(map(lambda x: x*10 + 10000000000 + int(str(sum(list(map(int, str(x).strip()))))[-1]) ,NE_Invoker_Pending.index)) #list(map(lambda x: '{datetime}_{sum}'.format(datetime= x.strftime('%Y%m%d%H%M%S'),sum = int(x.year)+int(x.month)*4+int(x.day)+int(x.hour)*70+int(x.minute)*10+int(x.second)),NE_Invoker_Pending.datetime))

# %%
NE_Invoker_Executed['datetime'] = NE_Invoker_Executed[0].str.split('.',expand=True)[0]
NE_Invoker_Executed[['Version','KindExecution','FeederList']] = NE_Invoker_Executed[0].str.split('"',expand=True)[[3,7,5]]
NE_Invoker_Executed.datetime = NE_Invoker_Executed.datetime.replace({'a':'AM','p':'PM'},regex=True)
NE_Invoker_Executed.datetime = list(map(lambda x: dt.datetime.strptime(x,'%d/%m/%Y %I:%M:%S %p'), NE_Invoker_Executed.datetime))
NE_Invoker_Executed = NE_Invoker_Executed.drop(columns=0).sort_values(by='datetime').reset_index(drop=True)
NE_Invoker_Executed['CustomId'] = '' #list(map(lambda x: '{datetime}_{sum}'.format(datetime= x.strftime('%Y%m%d%H%M%S'),sum = int(x.year)+int(x.month)*4+int(x.day)+int(x.hour)*70+int(x.minute)*10+int(x.second)),NE_Invoker_Executed.datetime))

for i in NE_Invoker_Pending.index:
    subset = NE_Invoker_Executed.loc[
                (NE_Invoker_Executed.FeederList == NE_Invoker_Pending.loc[i,'FeederList']) & 
                (NE_Invoker_Executed.Version== NE_Invoker_Pending.loc[i,'Version']) & 
                (NE_Invoker_Executed.Machine== NE_Invoker_Pending.loc[i,'Machine']) &
                (NE_Invoker_Executed.datetime >= NE_Invoker_Pending.loc[i,'datetime']) &
                (NE_Invoker_Executed.CustomId == ''),:]
    
    NE_Invoker_Executed.loc[subset.index.min(),'CustomId'] = NE_Invoker_Pending.loc[i,'CustomId']

NE_Invoker_Executions = NE_Invoker_Pending.merge(NE_Invoker_Executed[['CustomId','datetime']], on = 'CustomId', how='left').rename(columns={'datetime_x':'RecievedDatetime','datetime_y':'ExecutionDatetime'})
NE_Invoker_Executions['Executed'] = NE_Invoker_Executions['ExecutionDatetime'].notna()
# %%
extract_info['STATE_NAME'] = extract_info.merge(extract_state,left_on='EXTRACT_STATE_ID', right_on='ID', how = 'left')['STATE_NAME']
extract_info['SOURCE_NAME'] = extract_info.merge(extract_source,left_on='EXTRACT_SOURCE_ID', right_on='ID', how = 'left')['SOURCE_NAME']
changeset_info['STATE_NAME'] = changeset_info.merge(changeset_state,left_on='CHANGESET_STATE_ID', right_on='ID', how = 'left')['STATE_NAME']
changeset_info['SOURCE_NAME'] = changeset_info.merge(changeset_type,left_on='CHANGESET_TYPE_ID', right_on='ID', how = 'left')['TYPE_NAME']
changeset_hist['OLD_CHANGESET_STATE'] = changeset_hist.merge(changeset_state, left_on='OLD_CHANGESET_STATE_ID', right_on='ID', how='left')['STATE_NAME']
changeset_hist['NEW_CHANGESET_STATE'] = changeset_hist.merge(changeset_state, left_on='NEW_CHANGESET_STATE_ID', right_on='ID', how='left')['STATE_NAME']

# %%
print("Se conecta y realiza copiado a la db3")
sub_execution_time = time.time()
connection = sqlite3.connect(r'\\10.240.160.176\g$\Data\GIS-ADMS_CSRepo.db3')

# %%
NE_Invoker_Pending.to_sql('NE_Pending_Jobs',con=connection, if_exists='replace')
NE_Invoker_Executed.to_sql('NE_Executed_Jobs',con=connection, if_exists='replace')
NE_Invoker_Executions.to_sql('NE_Invoker_Executions_Jobs',con=connection, if_exists='replace')
extract_info.to_sql('Extracts',con=connection,if_exists='replace')
changeset_info.to_sql('Changesets',con=connection,if_exists='replace')
changeset_hist.to_sql('ChangesetsHistory',con=connection,if_exists='replace')
gis_jobs.to_sql('Jobs',con=connection,if_exists='replace')
changeset_extract_dependency.to_sql('Changeset_Extract_Dependency',con=connection,if_exists='replace')

print('Finalización copiado a la db3 en {}'.format(time.time()-sub_execution_time))

end_execution_time = time.time()
print('Finalización del Script en {}'.format(end_execution_time-start_execution_time))
# %%



# %%



