from tkinter import filedialog as fd
import requests, time, pandas as pd, datetime as dt

def Resent_messages():
    try:
        dataframe = pd.read_csv(fd.askopenfile(),sep=';')
        if ((dataframe.columns == 'KindExecution').any() & (dataframe.columns == 'FeederList').any() & (dataframe.columns == 'Version').any()):
                url = "http://10.240.160.176:8082/NetworkExporterExecutionService"
                body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://schneider-electric-dms.com/DMS/soa/schemas/NetworkExporterExecution/v1#" xmlns:v11="http://schneider-electric-dms.com/DMS/soa/schemas/StandardHeader/v1#">
                <soapenv:Header/>
                <soapenv:Body>
                    <v1:NetworkExporterExecution>
                        <v1:Header>
                        </v1:Header>
                        <v1:Payload>
                            <v1:Type>{kindExecution}</v1:Type>
                            <v1:FeederList>{Feederlist}</v1:FeederList>
                            <v1:Version>{version}</v1:Version>
                            <v1:Timestamp>{time}</v1:Timestamp>
                        </v1:Payload>
                    </v1:NetworkExporterExecution>
                </soapenv:Body>
                </soapenv:Envelope>
                """
                headers = {'Content-Type':'text/xml; charset=utf-8', 'SOAPAction':'http://schneider-electric-dms.com/DMS/soa/schemas/NetworkExporterExecution'}

                for i,x in dataframe.iterrows():
                    time.sleep(2)
                    response = requests.request("POST",url,data=body.format(kindExecution=x.KindExecution,Feederlist=x.FeederList,version=x.Version,time=dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')), headers=headers)
                    print("OK=",response.ok)
                    #print(body.format(kindExecution=x.KindExecution,Feederlist=x.FeederList,version=x.Version,time=dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
        else:
            print("Hpta brut@, el archivo csv debe contener las columnas 'KindExecution', 'FeederList','Version'")
    except UnicodeDecodeError:
        print("El archivo debe ser un .csv")
    
if __name__ == '__main__':
    Resent_messages()
