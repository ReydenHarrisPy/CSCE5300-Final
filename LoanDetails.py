from tkinter import *
import tkinter as tk
from tkinter import simpledialog
from tkinter import messagebox
from subprocess import call
test = Tk()
test.title('Eagle Credit Union')
test.geometry('900x600')
test.configure(bg='#fff')
test.resizable(True, True)




def close_window():
    test.destroy()




# labels for entry fields
creditlabel = Label(test, text="Enter your Credit Score", fg='black', bg='white', font=('Microsoft YaHei UI Light', 9))
creditlabel.place(x=100, y=154)

incomelable = Label(test, text="What is your Annual Income?", fg='black', bg='white',
                    font=('Microsoft YaHei UI Light', 9))
incomelable.place(x=100, y=254)

loanamountlabel = Label(test, text="Requested Loan Amount:", fg='black', bg='white',
                        font=('Microsoft YaHei UI Light', 9))
loanamountlabel.place(x=100, y=354)

loanlengthlabel = Label(test, text="Loan Tenure (Months)", fg='black', bg='white', font=('Microsoft YaHei UI Light', 9))
loanlengthlabel.place(x=100, y=454)

# entry fields for loan values

#had to define functions to convert values from strings to integers to run in loan validation script
def creditEntryint(x):
    my_int1.set(simpledialog.askinteger("Loan Application","Enter Credit Score:",parent=test))
my_int1=tk.IntVar()

creditEntry = tk.Entry(test, width=25,textvariable=my_int1, fg='black', border=2, bg='white', font=('Microsoft YaHei UI Light', 11))
creditEntry.place(x=100, y=180)
creditEntry.bind('<FocusIn>', creditEntryint)

def incomeEntryint(x):
    my_int2.set(simpledialog.askinteger("Loan Application","Enter Annual Income:",parent=test))
my_int2=tk.IntVar()

incomeEntry = Entry(test, width=25, textvariable=my_int2,fg='black', border=2, bg='white', font=('Microsoft YaHei UI Light', 11))
incomeEntry.place(x=100, y=280)
incomeEntry.bind('<FocusIn>', incomeEntryint)

def loanamountEntryint(x):
    my_int3.set(simpledialog.askinteger("Loan Application","Enter Loan Amount:",parent=test))
my_int3=tk.IntVar()
loanamountEntry = Entry(test, width=25, textvariable=my_int3,fg='black', border=2, bg='white', font=('Microsoft YaHei UI Light', 11))
loanamountEntry.place(x=100, y=380)
loanamountEntry.bind('<FocusIn>', loanamountEntryint)

def loanlengthEntryint(x):
    my_int4.set(simpledialog.askinteger("Loan Application","Enter Loan Length (Months):",parent=test))
my_int4=tk.IntVar()
loanlengthEntry = Entry(test, width=25,textvariable=my_int4, fg='black', border=2, bg='white', font=('Microsoft YaHei UI Light', 11))
loanlengthEntry.place(x=100, y=480)
loanlengthEntry.bind('<FocusIn>', loanlengthEntryint)

Button(test, width=39, pady=7, text='Apply', bg='green', fg='white', border=0, cursor='hand2',command=close_window).place(x=425, y=154)
# 'Crredit_Score', 'Annual_Income', 'Loan_Required', 'Loan_Tenure'



#instantiate variables to use in next program
x = creditEntry.get()
y = incomeEntry.get()
z = loanamountEntry.get()
a = loanlengthEntry.get()

test.mainloop()