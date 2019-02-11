import sqlite3
import pandas as pd
from tkinter import filedialog
import xml.etree.ElementTree as et
from pathlib import Path
import datetime

def table(df, name):
	conn = sqlite3.connect('1st.db')
	curs = conn.cursor()

	query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{tn}';".format(tn=name)
	cursor = conn.execute(query)
	result = cursor.fetchone()
	if result != None:
		df.to_sql(name, conn, if_exists="append", index=True)
		tab=pd.read_sql('SELECT * FROM "{tn}";'.format(tn=name), conn)
		tab.drop_duplicates(subset=['Data'], keep="last",  inplace=True)
		tab.set_index(['Data'], inplace=True)
		tab.sort_index(inplace=True)
		tab.to_sql(name, conn, if_exists="replace")

	else:
		df.to_sql(name, conn, if_exists="replace", index=True)

	conn.commit()
	conn.close()

def get_file_location():
	files_dir = filedialog.askopenfilenames()
	return files_dir

def import_xml():
	files=get_file_location()
	print(datetime.datetime.now())
	for file_to_parse in files:
		xx=parse_xml_file(file_to_parse)
		create_place_table(xx)
	print(datetime.datetime.now())

def parse_xml_file(file):
	parsed = et.parse(file)
	tree = parsed.getroot()
	return tree

def create_place_table(xx):
	punkty=["BIN-DEB", "BLK-JDW", "BYO-NOS", "CZK-TRZ", "GNI-PIS", "GOR-SUL", ]
	war=['Temperatura','Prędkość_wiatru','Kierunek_wiatru', 'Nasłonecznienie']
	for f_iter in xx:
		dictio={}
		count=0
		if f_iter.attrib['id'] in punkty:
			for s_iter in f_iter:
					#if s_iter.tag==war[count]:				
				z=datetime.datetime.strptime(f_iter.attrib["data"]+"00", '%Y%m%d%H%M')
				y=datetime.timedelta(hours=1)
				lista=[]
				data=[]

				for t_iter in s_iter:
					data.append(z)
					h=t_iter.text
					lista.append(h)
					z=z+y

				dictio['Data']=data
				dictio[s_iter.tag]=lista
					
				z=datetime.datetime.strptime(f_iter.attrib["data"]+"00", '%Y%m%d%H%M')
				count+=1

			gg=pd.DataFrame.from_dict(dictio)
			gg.set_index(['Data'], inplace=True)
			table(gg, f_iter.attrib['id'])
