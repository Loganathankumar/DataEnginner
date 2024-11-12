from pdf_mail import sendpdf


k = sendpdf("loganathankumar444@gmail.com",
            "loganathankumar444@gmail.com",
            "9731759045",
            "Resume",
            "pdf resume",
            "LoganathanResume",
            r"C:\Users\B12226\Documents")

k.email_send()