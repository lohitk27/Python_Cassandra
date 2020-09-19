import uuid
from cassandra.cluster import Cluster
from cassandra.cluster import EXEC_PROFILE_DEFAULT,ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy,DowngradingConsistencyRetryPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import threading
thread_local = threading.local()
#from cassandra.query import ordered_dict_factory

class Cassandra(object):
    #def __init__(self):
        #self.connection = self.connect()
        
    def connect(self):
        #auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        #profile = ExecutionProfile(WhiteListRoundRobinPolicy(['128.0.0.1','128.0.01.1']),DowngradingConsistencyRetryPolicy(),ConsistencyLevel.ONE,ConsistencyLevel.LOCAL_SERIAL,request_timeout=600)
        #cluster= Cluster(['128.0.0.1'],auth_provider=auth_provider)
        cluster = Cluster(['128.0.01.1'])
        #session = cluster.connect('Customerdata')
        #print("cass",session)
        thread_local.cassandra_session = session
        return thread_local.cassandra_session
        #return session
    
    def getDataFromTable(self,tableName, whereDocID = None):
        session = self.connection #self.connect()
        if whereDocID is None:
            query = SimpleStatement('SELECT * FROM ' + tableName + ';',consistency_level= ConsistencyLevel.ONE)
        else:
            query = "SELECT * FROM " + tableName + " WHERE docid = " + self.doublequote(whereDocID) + " ALLOW FILTERING;"
            #print query
            session.execute(query)
        rows = session.execute(query)
        #session.shutdown()
        return rows
    
    def InsertttoDocAdmin(self,df):
        #print('entered')
        session = self.connection#self.connect()
        for index,row in df.iterrows():
            docadmin_query = SimpleStatement('INSERT INTO doc_admin (docid,docname,isassigned,isprocessed) VALUES (%s,%s,%s,%s)',consistency_level= ConsistencyLevel.ONE)
            pdftoimg_query =  SimpleStatement('INSERT INTO doctoim (docid,docname,docimagepage,docimagelocation,doclocation) VALUES (%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.ONE)
            session.execute(docadmin_query, (str(row['docid']),str(row['docname']),bool(False),bool(False)))
            #print(int(row['docimagepage']))
            session.execute(pdftoimg_query, (str(row['docid']),str(row['docname']),int(row['docimagepage']),str(row['docimagelocation']),str(row['doclocation'])))
        #session.shutdown()
        
    def UpdateDocAdmin(self, docid, doctype):
        session = self.connection#self.connect()
        query = SimpleStatement('UPDATE doc_admin set doctype = %s where docid = %s;',consistency_level= ConsistencyLevel.ONE)
        query.consistency_level= ConsistencyLevel.ONE
        session.execute(query, (doctype,docid))
        #session.shutdown()
        
     
    def InsertttoTable(self,df):
        pass  # Temparorily returning 
        session = self.connection#self.connect()
        for index,row in df.iterrows():
            roi_query = SimpleStatement('INSERT INTO roitable (uid,docid,docname,pagenumber,linenumber,pagelocation,roinumber,roicode,roiheight,roiwidth,fontsize,lineheight,linewidth,roitopleftxloc,roitopleftyloc,linex,liney,tablecol,tablerow,isunderline,iscolor,text,pipelinename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.TWO)
            session.execute(roi_query, (uuid.uuid1(),str(row['docname']),str(row['docname']),int(row['pagenumber']),int(row['line_number']),str(row['pagelocation']),int(row['ROI_number']),int(row['ROI_code']),int(row['ROI_height']),int(row['ROI_width']),int(row['font_size']),int(row['line_height']),int(row['line_width']),int(row['ROItopleftx']),int(row['ROItoplefty']),int(row['line_x']),int(row['line_y']),int(row['table_column_number']),int(row['table_row_number']),bool(row['underline']),bool(row['color']),str(row['ocr_output']),str(row['pipelinename'])))
            spellcheck_query = SimpleStatement('INSERT INTO spellchecktable (uid,docid,docname,pagenumber,linenumber,pagelocation,roinumber,roicode,roiheight,roiwidth,fontsize,lineheight,linewidth,roitopleftxloc,roitopleftyloc,linex,liney,tablecol,tablerow,isunderline,iscolor,text,autospellcheck,pipelinename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.TWO)
            session.execute(spellcheck_query, (uuid.uuid1(),str(row['docname']),str(row['docname']),int(row['pagenumber']),int(row['line_number']),str(row['pagelocation']),int(row['ROI_number']),int(row['ROI_code']),int(row['ROI_height']),int(row['ROI_width']),int(row['font_size']),int(row['line_height']),int(row['line_width']),int(row['ROItopleftx']),int(row['ROItoplefty']),int(row['line_x']),int(row['line_y']),int(row['table_column_number']),int(row['table_row_number']),bool(row['underline']),bool(row['color']),str(row['ocr_output']),str(row['ocr_output']),str(row['pipelinename'])))
        #session.shutdown()
    
    def UpdatetoTable(self,df):
        session = self.connection #self.connect()
        for index,row in df.iterrows():
            roi_query = SimpleStatement('INSERT INTO roitable (uid,docid,docname,pagenumber,linenumber,pagelocation,roinumber,roicode,roiheight,roiwidth,fontsize,lineheight,linewidth,roitopleftxloc,roitopleftyloc,linex,liney,tablecol,tablerow,isunderline,iscolor,text,autocorrectedtext,pipelinename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.TWO)
            session.execute(roi_query, (uuid.uuid1(),str(row['docname']),str(row['docname']),int(row['pagenumber']),int(row['line_number']),str(row['pagelocation']),int(row['ROI_number']),int(row['ROI_code']),int(row['ROI_height']),int(row['ROI_width']),int(row['font_size']),int(row['line_height']),int(row['line_width']),int(row['ROItopleftx']),int(row['ROItoplefty']),int(row['line_x']),int(row['line_y']),int(row['table_column_number']),int(row['table_row_number']),bool(row['underline']),bool(row['color']),str(row['ocr_output']),str(row['corrected_text']),str(row['pipelinename'])))
            spellcheck_query = SimpleStatement('INSERT INTO spellchecktable (uid,docid,docname,pagenumber,linenumber,pagelocation,roinumber,roicode,roiheight,roiwidth,fontsize,lineheight,linewidth,roitopleftxloc,roitopleftyloc,linex,liney,tablecol,tablerow,isunderline,iscolor,text,autospellcheck,pipelinename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.TWO)
            session.execute(spellcheck_query, (uuid.uuid1(),str(row['docname']),str(row['docname']),int(row['pagenumber']),int(row['line_number']),str(row['pagelocation']),int(row['ROI_number']),int(row['ROI_code']),int(row['ROI_height']),int(row['ROI_width']),int(row['font_size']),int(row['line_height']),int(row['line_width']),int(row['ROItopleftx']),int(row['ROItoplefty']),int(row['line_x']),int(row['line_y']),int(row['table_column_number']),int(row['table_row_number']),bool(row['underline']),bool(row['color']),str(row['ocr_output']),str(row['corrected_text']),str(row['pipelinename'])))
        
    
    def InsertttoNLPTable(self, row):
        session = self.connection#self.connect()
        query = SimpleStatement('INSERT INTO nlptable (uid,docid,docname,pagenumber,linenumber,pagelocation,roinumber,roicode,roiheight,roiwidth,fontsize,lineheight,linewidth,roitopleftxloc,roitopleftyloc,linex,liney,tablecol,tablerow,isunderline,iscolor,pipelinename,' + row['AttributeName'] + ') VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',consistency_level= ConsistencyLevel.ONE)
        
        #print(query)
        session.execute(query, (uuid.uuid1(),str(row['docname']),str(row['docname']),int(row['pagenumber']),int(row['line_number']),str(row['pagelocation']),int(row['ROI_number']),int(row['ROI_code']),int(row['ROI_height']),int(row['ROI_width']),int(row['font_size']),int(row['line_height']),int(row['line_width']),int(row['ROItopleftx']),int(row['ROItoplefty']),int(row['line_x']),int(row['line_y']),int(row['table_column_number']),int(row['table_row_number']),bool(row['underline']),bool(row['color']),str(row['pipelinename']),row['AttributeValue']))
        #print(test)
        #session.shutdown()  
        
    def InserttoAttributeProbability(self,row):
        session = self.connection #self.connect()
        query = SimpleStatement('INSERT INTO attribute_probability (uid,docid, attribute ,nlpprobvalue) VALUES (%s,%s,%s,%s)',consistency_level= ConsistencyLevel.ONE)
        
        #print(query)
        session.execute(query,(uuid.uuid1(), str(row['docname']), str(row['AttributeName']).lower(),float(row['nlpprobvalue'])), timeout=30)
        #print(test)
        #session.shutdown()
        
    def GetConnection(self):
        return self.connection
        
    def CloseConnection(self):
        self.connection.shutdown()
        
        
        
        
        