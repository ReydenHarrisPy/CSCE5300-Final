import tkinter as tk
from tkinter import *
from tkinter import ttk
from pyhive import hive
from tkinter import *
import tabulate
import paramiko
import webbrowser
from tabulate import tabulate
from subprocess import call



class App:
    def __init__(self):
        db = hive.Connection(
            host="192.168.56.104",
            port=10000,
            username = "cloudera",
            password = "cloudera",
            database = "user",
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
            if tables[tab] == 'sales' and (
                    headers[i] == 'CUSTOMER_ID' or headers[i] == 'MAKE' or headers[i] == 'MODEL'):
                return TRUE
            return FALSE

        def getBlist(i):
            if headers[i] == 'CUSTOMER_ID':
                return 0
            elif headers[i] == 'MAKE':
                return 1
            elif headers[i] == 'MODEL':
                return 2

        def isClickable2(i):
            if (tables[tab] == 'parts_profit' or tables[tab] == 'parts') and (headers[i] == 'PARTS_ID'):
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
                    statement = "select * from customers"
                    cursor.execute(statement)
                    resultset = [item for item in cursor.fetchall()]
                    resultset.insert(0, "None")
                    blist1[0] = [item[0] for item in resultset]
                    blist1[0][0] = "None"
                    print(resultset)
                    entries.append(ttk.Combobox(showTable, values=resultset))
                    entries[i].grid(row=5, column=i)
                    continue
                elif headers[i] == 'MAKE':
                    statement = "select * from vehicle_information"
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
                    statement = "select * from customers"
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
                statement = "select * from parts"
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
         #   headers) - 1)
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

    comboExample = ttk.Combobox(app,
                                values=optionset)

    comboExample.grid(column=0, row=1)
    comboExample.current(0)

    logo = PhotoImage(file='C:\mascot.png')
    logo_widget = tk.Label(labelTop, image=logo, height=900, width=600)
    logo_widget.image = logo
    logo_widget.pack()

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

def LoanValidation():
    call(['Py', 'LoanValidation.py'])




def printProfile():
    call(['Py', 'printProfile.py'])


if __name__ == "__main__":
    while TRUE:
        firstOption = ["Apply for Loan", "Show Profile Information", ]

        option = comboBox(firstOption, "Eagle Credit Union")
        if option == 0:
            LoanValidation()
        elif option == 1:
            printProfile()


