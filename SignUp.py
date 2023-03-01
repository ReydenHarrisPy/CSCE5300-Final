from tkinter import *
from tkinter import messagebox
from pyhive import hive
#function to close window upon success
def signinpage():
    signup_window.destroy()
    import SignIn
#function to clear all signup entry fields
def clear():
    emailEntry.delete(0,END)
    User_IDEntry.delete(0,END)
    confirmlastnameEntry.delete(0,END)
    lastnameEntry.delete(0,END)
    phoneEntry.delete(0,END)
    ORGEntry.delete(0,END)

class App():
    def __init__(self):
        db = hive.Connection(
            host="192.168.56.102",
            port=10000,
            username = "cloudera",
            password = "cloudera",
            database = "default",
            auth='CUSTOM'
        )




#function to connect to db and insert new record
def connect_database():
    if  emailEntry.get()=='' or User_IDEntry.get()=='' or lastnameEntry=='' or confirmlastnameEntry=='':
        messagebox.showerror('Error','All Fields are required')
    elif lastnameEntry.get() != confirmlastnameEntry.get():
        messagebox.showerror('Error','Last name fields do not match')
    elif len(ORGEntry.get()) != 3:
        messagebox.showerror('Error','University code must be  3 letters')
    elif check.get()==0:
        messagebox.showerror('Error','Please agree to terms and conditions')
    else:
        try:
            db = hive.Connection(
            host="localhost",
            user="root",
            passwd="root",
            database="eagle_business_platform1"
            )
            cursor = db.cursor()
        except:
            messagebox.showerror('Database Connection Issue', 'Please contact DBA')
            return


        query1='default'
        cursor.execute(query1)

        #query to check if user ID exists
        query3='select * from users where id=%s'
        cursor.execute(query3, (User_IDEntry.get(),))
        row=cursor.fetchone()
        if row != None:
            messagebox.showerror('Error','User ID already exists')
        else:



            query='INSERT INTO customer_information(customer_id,last_name,email,phone,org) values(%s,%s,%s,%s,%s)'
            cursor.execute(query,(User_IDEntry.get(),lastnameEntry.get(),emailEntry.get(),phoneEntry.get(),ORGEntry.get()))


            db.commit()
            db.close()
            messagebox.showinfo('Success',"Registration Successful")
            clear()
            signup_window.destroy()
            import SignIn



signup_window=Tk()
signup_window.title('Eagle Credit Union')
signup_window.geometry('1500x1000')
signup_window.configure(bg='#fff')
signup_window.resizable(True,True)
img = PhotoImage(file='D:\mascot.png')
logoLabel = Label(signup_window,image=img,bg='white').place(x=50,y=50)

#sign in frame
frame=Frame(signup_window,bg='white')
frame.place(x=750,y=100)
#create heading within our signing frame
heading=Label(frame,text="Create an Account",fg='green4',bg= 'white',font=('Microsoft YaHei UI Light',25,'bold'))
heading.place(x=100,y=100)
heading.grid(row=0,column=0)

#email field
emailLabel=Label(frame,text='Email',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
emailLabel.grid(row=1,column=0,sticky='w',padx=25,pady=(20,0))

emailEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
emailEntry.grid(row=2,column=0)

#User_ID field

User_IDLabel=Label(frame,text='User ID (8 Digits ID)',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
User_IDLabel.grid(row=3,column=0,sticky='w',padx=25,pady=(20,0))

User_IDEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
User_IDEntry.grid(row=4,column=0)




#last_name filed
lastnameLabel=Label(frame,text='Last Name',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
lastnameLabel.grid(row=5,column=0,sticky='w',padx=25,pady=(20,0))

lastnameEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
lastnameEntry.grid(row=6,column=0)


confirmlastnameLabel=Label(frame,text='Confirm Last Name',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
confirmlastnameLabel.grid(row=7,column=0,sticky='w',padx=25,pady=(20,0))

confirmlastnameEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
confirmlastnameEntry.grid(row=8,column=0)



#phone field

phoneLabel=Label(frame,text='Phone #',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
phoneLabel.grid(row=9,column=0,sticky='w',padx=25,pady=(20,0))

phoneEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
phoneEntry.grid(row=10,column=0)


#ORG

ORGLabel=Label(frame,text='Organization (3 Letter University code)',bg='white',fg='green4',font=('Microsoft YaHei UI Light',18,'bold'))
ORGLabel.grid(row=11,column=0,sticky='w',padx=25,pady=(20,0))

ORGEntry=Entry(frame,width=25,font=('Microsoft YaHei UI Light',18,'bold'),fg='white',bg='green4')
ORGEntry.grid(row=12,column=0,padx=2)

#check button variable




check=IntVar()

#terms and conditions button

termsandconditions=Checkbutton(frame, text='I agree to the Terms & Conditions',font=('Microsoft YaHei UI Light',10,'bold'),bg='white',cursor='hand2',variable=check)
termsandconditions.grid(row=13,column=0)

signupButton=Button(frame,text='Sign Up',font=('Microsoft YaHei UI Light',10,'bold'),bd=0,bg='green4',fg='white',width=17,command=connect_database)
signupButton.grid(row=14,column=0,pady=10)


signinlabel=Label(frame,text="Already have an account?",fg='black',bg='white',font=('Microsoft YaHei UI Light',9))
signinlabel.grid(row=15,column=0,pady=10)

sign_in = Button(frame,width=6,text='Sign In',border=0,bg='white',cursor='hand2',fg='green',command=signinpage)
sign_in.place(x=335,y=690)



signup_window.mainloop()

