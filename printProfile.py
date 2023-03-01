import tkinter as tk
from tkinter import *
from pyhive import hive
from tabulate import tabulate
import webbrowser






root=Tk()
root.title('Enter Customer ID')
root.geometry('400x200')
root.configure(bg='#fff')
root.resizable(True,True)



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

def printSQL():

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

        query = ("""select balance,customer_lastname,recent_transaction,account_ID from balance_information where customer_ID=%s""")
        cursor.execute(query, (E.get(),))
        printReport(cursor, "Profile Information", ['Balance','Name','Recent Payment','Account ID'], f)

        query = (
            """select credit_score from customer_information where customer_ID=%s""")
        cursor.execute(query, (E.get(),))
        printReport(cursor, "Credit Score", ['Credit Score'], f)

        db.close()
        f.close()
        webbrowser.open("report.txt")
        root.destroy()



E = tk.Entry(root)
E.pack(anchor = CENTER)
B = Button(root, text = "Authenticate",command=printSQL)
B.pack(anchor = S)

root.mainloop()



