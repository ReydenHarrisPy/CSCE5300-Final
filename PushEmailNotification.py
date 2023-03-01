import smtplib, ssl
import csv
from email.message import EmailMessage
from email.utils import make_msgid
import mimetypes

# Credentials
Business_Account = 'sampleemailtest30@gmail.com'
Business_Account_Password = 'jibsyfxxyrecmbjl'


# Function to create and send an email
def email():
    with open('Customer_Data.csv') as csv_file:
        file = csv.reader(csv_file, delimiter=',')
        i = 0
        # Connecting to the mail server
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)

        # Validating the connection
        server.ehlo()

        # Logging in
        server.login(Business_Account, Business_Account_Password)

        # Creating the body of the email, omiting the first row as it is the header
        for row in file:
            if i > 0:
                to = [row[4]]
                body = "Hi" + str(row[1]) + ",\n\n" \
                       + "Thanks for banking with ous here is your monthly mortgage status report\n\nAccount_ID: " \
                       + str(row[0]) \
                       + "\n\nOutstanding Balance: " \
                       + str(row[2]) \
                       + "\n\nLast Transaction Date: " \
                       + str(row[3]) \
                       + "\n\nFor Further details please login to our website or mobile banking app." \
                       + "\n For any assistance reach out to our customer support team with the below mentioned numbers" \
                       + "\n\n Customer support: 1234567890 " \
                       + "\n\n Branch manager: 123456-Branch code"

                msg = EmailMessage()
                sender = Business_Account

                msg['Subject'] = 'Monthly mortgage update'
                msg['From'] = sender
                msg['To'] = to

                # set the plain text body
                msg.set_content(body)

                # now create a Content-ID for the image
                image_cid = make_msgid(domain='mortgage.com')

                # Create the msg along with image
                msg.add_alternative("""\
                <html>
                    <body>
                        <p>
                        Hi """ + str(row[1]) + """ ,<br>
                        </p>
                        <p>
                        Thanks for banking with ous here is your montly motgage status report<br>
                        </p>
                        <p>
                        <font color="#0CA0F5"> <b> Account_ID :</b></font> """ + str(row[0]) + """<br>
                        </p>
                        <p>
                        <font color="#0CA0F5"> <b> Outstanding Balance :</b></font> """ + str(row[2]) + """<br>
                        </p>
                        <p>
                        <font color="#0CA0F5"> <b> Last Transaction Date :</b></font> """ + str(row[3]) + """<br>
                        </p>
                        <p>
                        For Further details please login to our website or mobile banking app.
                        </p>
                        <p>
                        For any assistance reach out to our customer support team with the below mentioned numbers<br>
                        </p>
                        <p>
                        Customer support: 1234567890
                        </p>
                        <p>
                        Branch manager: 123456-Branch code
                        </p>
                        <img src="cid:{image_cid}">
                    </body>
                </html>
                """.format(image_cid=image_cid[1:-1]), subtype='html')

                with open('footer_IMG.jpg', 'rb') as img:
                    # know the Content-Type of the image
                    mtype, stype = mimetypes.guess_type(img.name)[0].split('/')

                    # attach it
                    msg.get_payload()[1].add_related(img.read(),
                                                     maintype=mtype,
                                                     subtype=stype,
                                                     cid=image_cid)

                # Declaring the sender and subject
                # sender = Business_Account
                # subject = 'Monthly mortgage update'

                # Creating the message of the email ( subject and body)

                # message = 'Subject: {}\n\n{}'.format(subject, body)

                # Sending the actual email
                server.sendmail(sender, to, msg.as_string())
            i += 1

        # Closing the connection
        server.close()


try:

    # Calling the email function to send mails
    email()

except Exception as e:
    print(e)
