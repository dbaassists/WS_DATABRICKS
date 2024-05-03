# Databricks notebook source

import warnings
import pandas as pd
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# COMMAND ----------

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: 	Etapa destinada para criação de função Python responsável pelo envio do e-mail com inconsistências.
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def envia_email(fromaddr, toaddr, assunto, msg):

    mensagem = MIMEMultipart() 
    mensagem['From'] = fromaddr 
    mensagem['To'] = toaddr 
    mensagem['Subject'] = assunto
    body = MIMEText(msg)
    mensagem.attach(body) 

    chave_email = "qiwb hnqt ucvn ehfx"

    s = smtplib.SMTP('smtp.gmail.com', 587) 
    s.starttls() 
    s.login(fromaddr, chave_email) 
    text = mensagem.as_string() 
    s.sendmail('noreply@google.com', toaddr, text) 
    s.quit()    

# COMMAND ----------

fromaddr = "dbaassists@gmail.com"
toaddr = "dbaassists@gmail.com"
assunto = "Erro Processamento Databricks - Inconsistências"
msg = "Prezado usuário,\n\nOcorreu um erro ao realizar o processamento do Job no Databricks. \n\nAtenciosamente,\n\nEquipe Suporte"

envia_email(fromaddr, toaddr, assunto, msg) 
