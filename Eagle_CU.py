import tkinter as tk
from tkinter import *
from tkinter import ttk
from pyhive import hive
from tkinter import *
from tabulate import tabulate
import paramiko
import webbrowser
from subprocess import call
from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql import session

def PreProcessing():
    from datetime import date
    from pyspark.sql import session
    from pyspark.sql import SparkSession
    # Creating a spark session
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Eagle_CU") \
        .getOrCreate()

    # Reading the file into dataframe
    filename = "balance_information.csv"
    df = spark.read.options(header='true', inferSchema='true') \
        .csv(filename)

    # Replacing null/no values with 0
    df = df.fillna(value=0)

    # Removing duplicate records
    df = df.dropDuplicates()

    # Removing rows with all values as 0
    df.na.drop("all")

    # Converting the spark dataframe to pandas dataframe
    x = df.toPandas()

    # Creating a file name with the actual file name and date appended to it
    temp_name = filename.replace('.csv', '')
    date = date.today()
    #s = str(temp_name) + str(date) + '.csv'

    # Creating a csv file with preprocessed data
    x.to_csv(r'C:\temp\balance_information.csv', index=False)

    exit()




class App:
    def __init__(self):
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username = "cloudera",
            password = "cloudera",
            database = "default",
            auth='CUSTOM'
        )

        cursor = db.cursor()
        cursor.execute("show tables")

        tables = [item[0] for item in cursor.fetchall()]
        print("Select a table:")

        for i in range(len(tables)):
            print(i, tables[i])

        tab = comboBox(tables, "Choose a table")
        statement = "describe " + tables[tab]
        print(statement)
        cursor.execute(statement)
        headers = [item[0] for item in cursor.fetchall()]

        print(headers)
        statement = "select * from " + tables[tab]
        cursor.execute(statement)
        result = [item for item in cursor.fetchall()]
        for i in range(len(result)):
            print(result[i])

        def show():
            listBox.delete(*listBox.get_children())
            statement = "select * from " + tables[tab]
            cursor.execute(statement)
            result = [item for item in cursor.fetchall()]
            tempList = result
            for i in range(len(tempList)):
                listBox.insert("", "end", values=result[i])

        def insert():
            statement = "Insert into " + tables[tab] + " values("
            temp = ''
            for i in range(len(headers)):
                temp = entries[i].get()
                if isClickable(i):
                    cp = entries[i].current()
                    temp = str(blist1[getBlist(i)][cp])
                if isClickable2(i):
                    cp = entries[i].current()
                    temp = str(blist2[cp])
                if temp == '' or temp == 'None':
                    temp = 'null'
                else:
                    temp = "'" + temp + "'"
                statement += temp
                statement += ", "
            statement = statement[:-2]
            statement += ")"
            print(statement)
            cursor.execute(statement)
            db.commit()
            show()

        backupRow = []

        def update():
            statement = "update " + tables[tab] + " set "
            temp = ''
            for i in range(len(headers)):
                temp = entries[i].get()
                if isClickable(i):
                    cp = entries[i].current()
                    temp = str(blist1[getBlist(i)][cp])
                if isClickable2(i):
                    cp = entries[i].current()
                    temp = str(blist2[cp])
                if temp == '' or temp == 'None':
                    temp = 'null'
                else:
                    temp = "'" + temp + "'"
                statement += headers[i] + "=" + temp + ", "
            statement = statement[:-2]
            statement += " Where "
            print(backupRow)
            for i in range(len(headers)):
                temp = backupRow[i]
                if temp == '' or temp == 'None':
                    temp = 'null'
                else:
                    temp = "'" + temp + "'"
                if temp == 'null':
                    statement += headers[i] + " is null and "
                    continue
                statement += headers[i] + "=" + temp + " and "
            statement = statement[:-4]
            print(statement)
            cursor.execute(statement)
            db.commit()
            show()

        def delete():
            statement = "delete from " + tables[tab] + " where "
            print(backupRow)
            tempres = ''
            for i in range(len(headers)):
                tempres = backupRow[i]
                if tempres == '' or tempres == 'None':
                    tempres = 'null'
                else:
                    tempres = "'" + tempres + "'"
                if tempres == 'null':
                    statement += headers[i] + " is null and "
                    continue
                statement += headers[i] + "='" + backupRow[i] + "' and "
            statement = statement[:-4]
            print(statement)
            cursor.execute(statement)
            db.commit()
            show()

        def onClick(event):
            backupRow.clear()
            item = listBox.identify("item", event.x, event.y)
            print(listBox.item(item, 'values'))
            ins = listBox.item(item, 'values')
            for i in range(len(ins)):
                print(ins[i])
                backupRow.append(ins[i])
                entries[i].delete(0, END)
                entries[i].insert(0, ins[i])

        def isClickable(i):
            if tables[tab] == 'customer_information' and (
                    headers[i] == 'CUSTOMER_ID' or headers[i] == 'customer_lastname' or headers[i] == 'age'):
                return TRUE
            return FALSE

        def getBlist(i):
            if headers[i] == 'customer_ID':
                return 0
            elif headers[i] == 'customer_lastname':
                return 1
            elif headers[i] == 'age':
                return 2

        def isClickable2(i):
            if (tables[tab] == 'balance_information' or tables[tab] == 'customer_information') and (headers[i] == 'customer_ID'):
                return TRUE
            return FALSE

        self.root = tk.Tk()
        self.tree = ttk.Treeview()

        showTable = self.root
        showTable.title("11046224 - Reyden Harris")
        label = tk.Label(showTable, text=tables[tab], font=('Microsoft YaHei UI Light',25,'bold')).grid(row=0, columnspan=len(headers))
        label2 = tk.Label(showTable, text="                      ").grid(row=4, columnspan=len(headers))
        label3 = tk.Label(showTable, text="                      ").grid(row=6, columnspan=len(headers))
        # create Treeview with 3 columns
        cols = headers
        self.tree = ttk.Treeview(showTable, columns=headers, show='headings')
        listBox = self.tree
        # set column headings
        for col in cols:
            listBox.heading(col, text=col)
        listBox.grid(row=10, column=0, columnspan=len(headers))
        show()
        entries = []

        blist1 = [[], [], []]
        blist2 = []
        for i in range(len(headers)):
            if isClickable(i):
                if headers[i] == 'CUSTOMER_ID':
                    statement = "select * from customer_information"
                    cursor.execute(statement)
                    resultset = [item for item in cursor.fetchall()]
                    resultset.insert(0, "None")
                    blist1[0] = [item[0] for item in resultset]
                    blist1[0][0] = "None"
                    print(resultset)
                    entries.append(ttk.Combobox(showTable, values=resultset))
                    entries[i].grid(row=5, column=i)
                    continue
                elif headers[i] == 'customer_lastname':
                    statement = "select * from customer_information"
                    cursor.execute(statement)
                    resultset = [item for item in cursor.fetchall()]
                    resultset.insert(0, "None")
                    blist1[1] = [item[1] for item in resultset]
                    blist1[1][0] = "None"
                    print(resultset)
                    entries.append(ttk.Combobox(showTable, values=resultset))
                    entries[i].grid(row=5, column=i)
                    continue
                elif headers[i] == 'CUSTOMER_ID':
                    statement = "select * from customer_information"
                    cursor.execute(statement)
                    resultset = [item for item in cursor.fetchall()]
                    resultset.insert(0, "None")
                    blist1[2] = [item[0] for item in resultset]
                    blist1[2][0] = "None"
                    print(resultset)
                    entries.append(ttk.Combobox(showTable, values=resultset))
                    entries[i].grid(row=5, column=i)
                    continue
            elif isClickable2(i):
                statement = "select * from balance_information"
                cursor.execute(statement)
                resultset = [item for item in cursor.fetchall()]
                k = 0
                lim = len(resultset)
                while k < lim:
                    if resultset[k][2] == 'inactive':
                        del resultset[k]
                        lim -= 1
                        k -= 1
                    k += 1
                resultset.insert(0, "None")
                blist2 = [item[0] for item in resultset]
                blist2[0] = 'None'
                print(resultset)
                print(blist2)
                entries.append(ttk.Combobox(showTable, values=resultset))
                entries[i].grid(row=5, column=i)
                continue

            entries.append(Entry(self.root))
            entries[i].grid(row=5, column=i)

        #updateButton = tk.Button(showTable, text="Update Table", width=15, command=update).grid(row=1, column=0)
        closeButton = tk.Button(showTable, text="Close App", width=15, command=exit).grid(row=3,
                                                                                          column=len(headers) - 1)
        #deleteButton = tk.Button(showTable, text="Delete Record", width=15, command=delete).grid(row=1, column=len(
          #  headers) - 1)
        insertButton = tk.Button(showTable, text="Insert Record", width=15, command=insert).grid(row=3, column=0)

        listBox.bind("<ButtonRelease-1>", onClick)
        self.root.mainloop()
        db.close()


def comboBox(optionset, title):
    global option
    option = 0
    app = tk.Tk()
    app.geometry('1500x800')

    labelTop = tk.Label(app)
    labelTop.grid(column=0, row=0)
    #drop down box menu fuction and details
    comboExample = ttk.Combobox(app,
                                values=optionset)

    comboExample.grid(column=0, row=1)
    comboExample.current(0)

    logo = PhotoImage(file='C:\mascot.png')
    logo_widget = tk.Label(labelTop, image=logo, height=900, width=600)
    logo_widget.image = logo
    logo_widget.pack()
    #menu function that allows for selections to be made
    def Menu():
        global option
        option = comboExample.current()
        app.destroy()

    optionButton = tk.Button(app, text="OK", width=15, command=Menu).grid(row=2, column=0)
    exitButton = tk.Button(app, text="Exit", width=15, command=exit).grid(row=3, column=0)
    app.grid_rowconfigure(0, weight=1)
    app.grid_columnconfigure(0, weight=1)

    app.title(title)
    app.mainloop()
    return option

#function to print out query results on the 'notepad'
def printReport(cursor, title, headersnames, f):
    f.write(title)
    f.write("\n")
    print(title)
    myresult = cursor.fetchall()
    finalres = tabulate(myresult, headers=headersnames, tablefmt='psql')
    print(finalres)
    f.write(finalres)
    f.write("\n\n\n")
    print("\n")

#function that connnects to hive server/DB and executes our queries
def generateReports():
    db = hive.Connection(
        host="192.168.56.104",
        port=10000,
        username="cloudera",
        password="cloudera",
        database="default",
        auth='CUSTOM'
    )
    cursor = db.cursor()

    f = open("report.txt", "w")

    cursor.execute("""select avg(balance) as average_loan from balance_information""")
    printReport(cursor, "Average Customer Loan Balance", ['Balance'], f)

    cursor.execute("""select avg(age) from customer_information""")
    printReport(cursor, "Average Customer Age", ['Age'], f)

    cursor.execute("""select avg(credit_score) from customer_information""")
    printReport(cursor, "Average Customer Credit Rating", ['Credit Score'], f)

    cursor.execute("""select count(*) from customer_information""")
    printReport(cursor, "Number of Clients", ['Number of clients'], f)




    db.close()
    f.close()
    webbrowser.open("report.txt")
    call(['Py','Plot.py'])

#function to incorporate the scp command to load our new data into database
def SCPBalance():
    from paramiko import SSHClient
    from scp import SCPClient

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname='192.168.56.104',
                username='cloudera',
                password='cloudera')

    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())
    #scp new script from preprocessing output
    scp.put(r'C:\temp\balance_information.csv', '/home/cloudera/temp/')

    scp.close()





def LoadNewBalanceInfo():
    try:
        PreProcessing()
    except:
        pass
    try:
        SCPBalance()
    except:
        print("scp balance error")



    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.56.104', username='cloudera', password='cloudera')
        stdin, stdout, stderr = client.exec_command(
        'hdfs dfs -put /home/cloudera/temp/balance_information.csv /project/data/')
        for line in stdout:
            print(line.strip('\n'))
    except:
        print("error moving from cloudera local to hadoop")
    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query = ("DROP TABLE IF EXISTS temp_balance")
        cursor.execute(query)
        db.commit()
        db.close()
    except:
        print('error dropping temp table')

    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query = ("CREATE TABLE temp_balance LIKE balance_information")
        cursor.execute(query)
        db.commit()
        db.close()
    except:
        print('error creating temp hive table')
    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query = ("load data inpath '/project/data/balance_information.csv' into table temp_balance")
        cursor.execute(query)
        db.commit()
        db.close()
    except:
        print('error loading hive table')

    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query = ("DROP TABLE balance_information")
        cursor.execute(query)
        db.commit()
        db.close()
    except:
        print('error dropping original hive table')

    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query = ("ALTER TABLE temp_balance RENAME TO balance_information")
        cursor.execute(query)
        db.commit()
        db.close()
    except:
        print('error renaming hive table')

    try:
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username="cloudera",
            password="cloudera",
            database="default",
            auth='CUSTOM'
        )
        cursor = db.cursor()
        query1=('ALTER TABLE balance_information SET TBLPROPERTIES ("skip.header.line.count"="1")')
        cursor.execute(query1)
        db.commit()
        db.close()
    except:
        print('error changing hive table properties')
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.56.104', username='cloudera', password='cloudera')

        stdin, stdout, stderr = client.exec_command(
            'hdfs dfs -get /project/data/balance_information.csv /home/cloudera/temp/')
        for line in stdout:
            print(line.strip('\n'))
    except:
        print('Unable to rename file')

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.56.104', username='cloudera', password='cloudera')

        stdin, stdout, stderr = client.exec_command(
            'mv /home/cloudera/temp/balance_information.csv /home/cloudera/temp/balance_information_$(date +%d-%m-%Y).csv')
        for line in stdout:
            print(line.strip('\n'))
    except:
        print('Unable to rename file')




    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.56.104', username='cloudera', password='cloudera')
        stdin, stdout, stderr = client.exec_command('hdfs dfs -put /home/cloudera/temp/balance_information* /project/data/ARCHIVE/')
        for line in stdout:
            print(line.strip('\n'))



    except:
        pass
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.56.104', username='cloudera', password='cloudera')
        stdin, stdout, stderr = client.exec_command('rm /home/cloudera/temp/balance_information*')
        for line in stdout:
            print(line.strip('\n'))



    except:
        pass







def EmailNotification():
    call(['Py', 'PushEmailNotification.py'])




if __name__ == "__main__":
    while TRUE:
        firstOption = ["Show and Edit tables", "Generate Reports", "Load New Data","Email Clients"]

        option = comboBox(firstOption, "Eagle Credit Union")
        if option == 0:
            app = App()
        elif option == 1:
            print('Generating Reports, Please be patient')
            generateReports()
        elif option == 2:
            print("Loading New Data")
            LoadNewBalanceInfo()
        elif option == 3:
            print('Sending Emails to Clients')
            EmailNotification()
