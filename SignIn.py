from tkinter import *
from tkinter import messagebox
from pyhive import hive
from subprocess import call








def Eagle_CU():
    root.destroy()
    call(['Py', 'Eagle_CU.py'])

def Eagle_CU_User():
    root.destroy()
    call(['Py', 'Eagle_CU_User.py'])

# create login window
root=Tk()
root.title('Eagle Credit Union')
root.geometry('1800x1000')
root.configure(bg='#fff')
root.resizable(True,True)

#define our sign in function


def login_admin():
    if userEntry.get()=='' or passwordEntry.get()=='':
        messagebox.showerror('Error','All fields required')
    else:
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
        except:
            messagebox.showerror('Database Connection Issue', 'Please contact DBA')
        try:


            cursor = db.cursor()
            query1 = 'use default'
            cursor.execute(query1)

            query='select * from admin_login where customer_ID=%s and password=%s'
            cursor.execute(query,(userEntry.get(),passwordEntry.get()))
            row = cursor.fetchone()
            if row==None:
                messagebox.showerror('Error','Invalid ID or password')
            else:
               Eagle_CU()
        except:
            messagebox.showerror("Error",'')


def login_user():
    if userEntry.get()=='' or passwordEntry.get()=='':
        messagebox.showerror('Error','All fields required')
    else:
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
        except:
            messagebox.showerror('Database Connection Issue', 'Please contact DBA')
        try:


            cursor = db.cursor()
            query1 = 'use default'
            cursor.execute(query1)

            query='select * from user_login where customer_id=%s and password=%s'
            cursor.execute(query,(userEntry.get(),passwordEntry.get()))
            row = cursor.fetchone()
            if row==None:
                messagebox.showerror('Error','Invalid ID or password')
            else:
                Eagle_CU_User()
        except:
            messagebox.showerror("Error",'')










#import login window image
img = PhotoImage(file='C:\mascot.png')
Label(root,image=img,bg='white').place(x=50,y=50)

#sign in frame
frame=Frame(root,width=700,height=750,bg='white')
frame.place(x=900,y=100)
#create heading within our signing frame
heading=Label(frame,text="Eagle Credit Union \n Please Sign In",fg='green4',bg= 'white',font=('Microsoft YaHei UI Light',23,'bold'))
heading.place(x=100,y=90)

#create user login field

#define function that create disappearing letters
def on_enter(e):
    userEntry.delete(0,'end')

def on_leave(e):
        name=userEntry.get()
        if name=='':
            userEntry.insert(0,'Customer ID')



#entry field for customer ID

userEntry = Entry(frame, width=25,fg='black',border=2, bg='white', font=('Microsoft YaHei UI Light',11))
userEntry.place(x=100,y=180)
userEntry.insert(0,'Customer ID')
userEntry.bind('<FocusIn>', on_enter) #calls function above to add feature of disappearing entries
userEntry.bind('<FocusOut>', on_leave)
Frame(frame,width=550,height=2,bg='black').place(x=90,y=240)



#create password field
def on_enter(e):
    passwordEntry.delete(0,'end')

def on_leave(e):
        code=passwordEntry.get()
        if code=='':
            passwordEntry.insert(0,'Password')


#create entry field for password
passwordEntry = Entry(frame, width=25,fg='black',border=2, bg='white', font=('Microsoft YaHei UI Light',11))
passwordEntry.place(x=100,y=380)
passwordEntry.insert(0,'password')
passwordEntry.bind('<FocusIn>', on_enter) #calls function above to add feature of disappearing entries
passwordEntry.bind('<FocusOut>', on_leave)


Frame(frame,width=550,height=2,bg='black').place(x=90,y=440)

#create sign in button
Button(frame,width=39,pady=7,text='Log In',bg='green',fg='white',border=0,cursor='hand2',command=login_user).place(x=75,y=454)

signuplabel=Label(frame,text="Want to Sign in as Admin?",fg='black',bg='white',font=('Microsoft YaHei UI Light',9))
signuplabel.place(x=100,y=554)

sign_up = Button(frame,width=10,text='Admin Access',border=0,bg='green4',cursor='hand2',fg='white',command=login_admin)
sign_up.place(x=260,y=550)

root.mainloop()



