# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name='firstname',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name' )

# COMMAND ----------

dbutils.widgets.dropdown(name='firstnamedropdown',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name dropdown' )

# COMMAND ----------

dbutils.widgets.multiselect(name='firstnamems',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name MS' )

# COMMAND ----------

dbutils.widgets.text(name='firstnametext',defaultValue='Sathya',label='Name Textbox' )

# COMMAND ----------

dbutils.widgets.get('firstname')

# COMMAND ----------

dbutils.widgets.getArgument('firstnamems')

# COMMAND ----------

dbutils.widgets.remove('firstnamems')
