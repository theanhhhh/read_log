import csv
import cx_Oracle
#import pandas as pd

ACCESS_FILE = './access.log'
output = []

if __name__  == "__main__":
    with open(ACCESS_FILE) as f:
        lines = f.readlines()
    
    for row, line in enumerate(lines):
        line = line.strip()
        columns = line.split('\t')
        if(len(columns) > 5):
            l_date      = columns[0]
            l_time      = columns[1]
            l_url_id    = columns[5]
            l_ip        = columns[8]
            l_ip_addr   = l_ip.strip('""')

            #if '/xmlpserver/servlet/xdo?_xdo=&#37;2FPHAN_HE_GL&#37;2FReports&#37;2FGL002F.xdo&amp;fromLoadingPage=true&amp;_id=fd5104b8-1461-4ff6-98a4-b9e0cad57a32' not in l_url_id:
            if 'PHAN_HE' not in l_url_id:
                l_url_id = None
                #print(type(l_url_id))

            output.append([
                l_date,
                l_time,
                l_url_id,
                l_ip_addr
            ])


    with open('output_access.csv', 'w', newline = '') as csvfile:
        csv_output = csv.writer(csvfile)
        csv_output.writerows(output)

#insert database
try: 
    connection = cx_Oracle.connect('dwprod/oracle123@192.168.49.32:1521/uatdwh')  
#execute the sqlquery 
    cursor = connection.cursor() 
    #insert theo biến      
    #cursor.execute("INSERT INTO DWH_LOGGING(CREATE_DATE, CREATE_TIME, URL_ID, IP_ADDR) VALUES (:l_date, :l_time, :l_url_id, :l_ip_addr)", [l_date, l_time, l_url_id, l_ip_addr])
    with open("output_access.csv", "r") as csv_file:
        #csv_reader = csv.reader(csv_file)
        #for line in csv_reader:
        #    output.append(line)
        for line in output:
            print(line)
            #Tạo bảng logging
            #cursor.execute("CREATE TABLE DWH_LOGGING(CREATE_DATE VARCHAR2(50), CREATE_TIME VARCHAR2(50), URL_ID VARCHAR2(500), IP_ADDR VARCHAR2(50))")
            #insert theo file csv theo line
            #insert_string = "INSERT INTO DWH_LOGGING(CREATE_DATE, CREATE_TIME, URL_ID, IP_ADDR) VALUES (:0, :1, :2, :3)"
            #cursor.execute(insert_string, line)
            cursor.execute("INSERT INTO DWH_LOGGING(CREATE_DATE, CREATE_TIME, URL_ID, IP_ADDR) VALUES (:0, :1, :2, :3)", line)       
            connection.commit()
            print("successful")
      
except cx_Oracle.DatabaseError as e: 
    print("There is a problem with Oracle", e)
finally: 
    if cursor:
        cursor.close() 
    if connection:
        connection.close() 
    
