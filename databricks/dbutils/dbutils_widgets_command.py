# Databricks notebook source
dbutils.widgets.help()

dbutils.widgets.combobox(name='firstname',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name' )


dbutils.widgets.dropdown(name='firstnamedropdown',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name dropdown' )

dbutils.widgets.multiselect(name='firstnamems',defaultValue='Sathya',choices=['Sathya','Priya','Sandhya'],label='Name MS' )


dbutils.widgets.text(name='firstnametext',defaultValue='Sathya',label='Name Textbox' )


dbutils.widgets.get('firstname')


dbutils.widgets.getArgument('firstnamems')


dbutils.widgets.remove('firstnamems')
