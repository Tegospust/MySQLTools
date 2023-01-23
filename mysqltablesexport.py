#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  mysqltablesexport.py
#  
#  Copyright 2017 AndroidAppsPlatform <AndroidAppsPlatform@OUNIS-KOMPUTER>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

#!/usr/bin/python

import MySQLdb
import math
import sys, traceback

#constants
DEF_HOST = "useastdb.ensembl.org"  #"ensembldb.ensembl.org" "useastdb.ensembl.org"
DEF_USER = "ounis"
DEF_PASSW = ""
DEF_DBASE = "mysql"
DEF_PORT = 3306

SERVER_localhost = {'Address' : '127.0.0.1', 'User' : 'ounis'}
SERVER_ensembldb_ensembl_org = {'Address' : 'ensembldb.ensembl.org', 'User' : 'anonymous' }
SERVER_useastdb_ensembl_org = {'Address' : 'useastdb.ensembl.org', 'User' : 'anonymous'}


SERVER_LIST = []
SERVER_LIST.append(SERVER_localhost)
SERVER_LIST.append(SERVER_ensembldb_ensembl_org)
SERVER_LIST.append(SERVER_useastdb_ensembl_org)

DEF_SERVER = SERVER_LIST[0]

CONN_STRING = ""

SQL_SHOW_DATABASES = "show databases"
SQL_USE_DATABASE = "use %s"
SQL_SHOW_TABLES = "show tables"
SQL_EXP_QRY = "select * from %s %s"
SQL_EXP_COUNT = "select count(*) as row_count from %s"


MENU_FORMAT = " %d. %s"

EXP_TXT = "TXT"
EXP_HTML = "HTML"

FILE_NAME_FORMAT = "%s_%s%s"

EXPORT_FILE_BASE = "export"

EXT_HTML = ".html"
EXT_TXT = ".txt"

HTML_LT = "&lt;"
HTML_GT = "&gt;"

MENU_MAX_LINES_ON_PAGE = 25

EXIT_CODE_EXPORT_ERROR = 10
EXTT_CODE_DBASE_LIST_ERR = 1
EXIT_CODE_DBASE_NOT_AVAIL = 2
EXIT_CODE_TABLE_NOT_AVAIL = 3
EXIT_CODE_TABLE_LIST_ERR = 4

# stałe wykorzystywa
END_LINE_SEP = "\n"
DICT_VALUES_SEP = ":"
DICT_ITEMS_SEP = ","
        
CONN_DATA_SAVE_FORMAT = "Address"+ DICT_VALUES_SEP +"%s"+ DICT_ITEMS_SEP +"User"+ DICT_VALUES_SEP +"%s" + END_LINE_SEP





# global vars
DBConnect = None
cursor = None
DBases = []
Tables = []
FieldNames = []

# tutaj przechowujemy dane aktualnego użytkownika
User = {"Name" : "", "Passw" : None}




def getTableRowCount(aConnect, aTable):
	result = -1
	if (not aConnect == None):
		cursor = aConnect.cursor()
		if (not cursor == None):
			sql = (SQL_EXP_COUNT % aTable)
			try:
				cursor.execute(sql)
				rows = cursor.fetchall()
				row = rows[0]
				result = row[0]

			except Exception as e:
				print(f'Bład zliczania ilości rekordów dla tabeli: {aTable}\n - {e}')

	return result

def printline(pattern ="=", length=35):
	print(pattern *length)

def prepareHost(aServer):
	result = aServer['Address']
	
	return result
	

# połączenie z serwerem
def Connect(aHost, aUser, dbase, aPort):
	_user = aUser
	_passw = ""
	if aUser['Passw'] is None:
		_user = input("Użytkownik(def.:%s): " % (aUser['Name']))
		if len(_user) == 0:
			_user = aUser['Name']
		_passw = input("Hasło: ")
	else:
		_user = aUser['Name']
		_passw = aUser['Passw']
	print("Łączenie z serwerem... ") 
	_db = None
	try:
		# connect(host="", port=3306, user="", pass="", db="")
		
		_db = MySQLdb.connect(host=aHost, port=aPort,user=_user, passwd=_passw, db=dbase)
		# BEBUG: print(_user)
	except Exception as e:
		print("Błąd połączenia z serwerem: %s - %s" % (e.args[0], e.args[1]))
		printline("-", 88)
		traceback.print_exc(file=sys.stdout)
		printline("-", 88)


	if _db is not None:
		print("Gotowe")
	aUser['Name'] = _user
	aUser['Passw'] = _passw
	return _db
# end of Connect() def

# ustawienie wybranej bazy danych
# def UseDBase(dbaseName):
# 	Cursor.execute(SQL_USE_DATABASE % (dbaseName))
# end of UseDBase() def

# połączenie z serwerem z wyborem konkretnej bazy danych
def Reconnect(dbaseName):
	_db = None
	try:
		print("Reconnect: User = %s" % (User['Name']))
		_db = MySQLdb.connect(DEF_HOST, User['Name'], User['Passw'], dbaseName)
	except Exception as e:
		print("Błąd podczas połączenia z: %s" % (dbaseName))
		
	return _db
# end of Reconnect() def

# listowanie baz danych
# 
# zwraca tablicę/listę baz danych z serwera
def GetDatabases(conn):
	print("Ściąganie list baz danych... ")
	dbases = None

	Cursor = conn.cursor()
	try:
		Cursor.execute(SQL_SHOW_DATABASES)
		rows = Cursor.fetchall()
	except:
		rows = None
	if (rows is not None):	
		print("Gotowe")
		dbases = []	
		for i in range(len(rows)):
			dbases.append(rows[i][0])
		print("Liczba baz danych: %d" % (len(rows)))
	return dbases
# end of ShowDatabases() def

# listowanie tabel
#
# zwraca tablice/liste tabel dla bazy danych poprze aktualne połączenie
# @conn - aktualne połączenie
def GetTables(conn):
	print("Ściąganie listy tabel...")
	Cursor = conn.cursor()
	rows = None
	tables = None
	# DEBUG: print(Cursor)
	try:
		Cursor.execute(SQL_SHOW_TABLES)
		rows = Cursor.fetchall()
	except Exception as e:
		#print ("Coś nie tak w GetTables(): %s %s" % (e.args[0], e.args[1]))
		rows = None
	if (rows is not None):
		tables = []
		for i in range(len(rows)):
			tmp = rows[i][0]
			tables.append(tmp)
	return tables
# end of ShowTables() def

# wyświetlanie menu wyboru opcji
# @opArr - lista listy opcji 
# @aTittle - nagłówek menu
# 
# zwraca numer wybranej opcji 	 0 - do obsłużenia jako rezygnacja 
#								 None - coś nie tak
def ChooseOption(opArr, aTittle, aConnect=None) -> int:
	k = None
	choosen_number = False
	if opArr is not None:
		print()

		printline("=", 35)
		print(aTittle)
		printline("=",35)
		for i in range(len(opArr)):

			row_count_txt = ""
			if (not aConnect == None):
				row_count_txt = f' ({str(getTableRowCount(aConnect,opArr[i]))})'

			print(f'{MENU_FORMAT % (i + 1, opArr[i])}' + f'{row_count_txt}')
			if ((i+1) % (MENU_MAX_LINES_ON_PAGE - 2) == 0):
				k = input("Wpisz numer(1 - %d)([ENTER] - przewiń dalej):" % (len(opArr)))
				if k.isnumeric() and (int(k) >= -1 and int(k) <= len(opArr)):
					choosen_number = True
					break
				else:
					print("Wartość spoza zakresu")
		if (not choosen_number):
			print("\n0. Wyjście")
			printline()
			answ = input("> ")
			if answ.isnumeric():
				choosen = int(answ) - 1
				if choosen >= -1 and choosen <= len(opArr) - 1:
					return choosen
				else:
					return None
			else:
				return None
		else:
			return int(k)-1
			#choosen = k
	else:
		return None
# end of ChooseOption() def

# tworzenie nazwy pliku wykorzystywanego w exporcie
# @tabName - wybrana nazwa tabeli
# @fileExt - rozszerzenie pliku exportu
def createFileName(tabName, fileExt):
	# zapełnianie parametrami wzorca formartu nazwy pliku
	s = (FILE_NAME_FORMAT % (EXPORT_FILE_BASE, tabName, fileExt))
	return s

# wycinanie z nazwy skryptu pythona rozszerzenia ".py"
def getScriptName():
	return sys.argv[0][0:len(sys.argv[0]) - len(".py")]

# odpala zapytanie i przepisuje wyniki do listy
# @aConn - połączenie
# @aSQLCl - treść zapytania z parametrami
# @aTable - tabela param. zapytania
# @aLimit - zakres wyświetlanych danych
def GenExpSQL(aConn, aSQLCl, aTable, aLimit):
	limit_par = ""
	rows = None
	num_fields = 0
	print("Zapytanie odpalone. Proszę czekać... ")
	Cursor = aConn.cursor()
	if aLimit is not None:  # limit podany -1 wszystko bądź kontretna liczba
		if (aLimit > 0):
			limit_par = ("limit %d" % (aLimit))
	# if (len(limit_par) > 0):
		_sql = (aSQLCl % (aTable, limit_par)) # limit = None czyli całkowicie beze tego parametru
	else:
		_sql = (aSQLCl % aTable)

	try:
		#print("I")
		Cursor.execute(_sql)
		#rows = Cursor.fetchall()
		#num_fields = len(Cursor.description)
		#aFieldNames = [i[0] for i in Cursor.description]
		print("Gotowe")
	except:
		print("Błąd zapytania SQL dla tabeli %s" % (aTable))
	
	#print(num_fields)
	#print(aFieldNames)
	#print("II")
	return Cursor

# pobiera nagłówek tabeli z aktualnego cursora
# @aktualny kursor	
def getTableHeader(cursor):
	header = []# None
	print("Pobieranie nagłówka tabeli...", end='')
	try:
		#
		# patent wzięty z: https://stackoverflow.com/questions/5010042/mysql-get-column-name-or-alias-from-query
		#
		if (not cursor.description is None):
			num_fields = len(cursor.description)
		#print("Liczba kolumn: %d" % (num_fields))
		# DEBUG: print("\n************************\n")
		# DEBUG: print(cursor.description)
		# DEBUG: print("\n************************\n")
		# for tp in (cursor.description):
		# 	header.append(tp[0])
			header = [i[0] for i in cursor.description]
		print("Gotowe")
	except Exception as e:
		print("getTableHeader()\nBłąd pobierania nagłówka tabeli z zapytania: " + str([i for i in e.args]))
	
	return header

def getTableRows(cursor):
	data = None
	print("Pobieranie danych tabeli...", end='')
	try:
		data = cursor.fetchall()
		print("Gotowe")
	except Exception as e:
		print("getTableRows()\nBłąd pobierania danych tabeli z zapytania: %s, cursor: %s " % (e.args[0], str(cursor)))
	return data
	
# zapis danych do pliku .txt
# @fName - nazwa pliku
# @header - lista kolumn
# @data - lista danych z tabeli	
def export_to_txt(fName, header, data):

	#
	# TODO: dodać formatowanie danych w pliku tekstowym
	#
	# DEBUG: 
	#rc = 1
	#for r in (data):
		#s = r
		#print("row %d: %s" % (rc,s))
		#rc += 1	
		
	result = True
	print("Eksport danych do pliku .txt... ")
	try:
		r_count = 1;
		f = open(fName, "w", encoding="utf-8")
		s = "lp., "
		for i in range(len(header)):
			s += header[i] + ", "
		# DEBUG print(s)
		# sortowanie
		# data = sorted(data, key=lambda atable: atable[2])
		f.write(s + "\n")
		for row in (data): #sorted(data, key=lambda atable: atable[1]):
			s = str(r_count) + ". "
			for i in range(0, len(row)):
				s += str(row[i]) + ", "
			f.write(s + "\n");

			#print("%s" % (str(s)))
			r_count += 1
		f.flush()
		f.close()
	except Exception as e:
		eM = [i for i in e.args]
		print("Błąd podczas generowania pliku .txt: %s" % (eM))
		result = False
	else:
		print("Gotowe")
	return result
#end of def export2Txt()

# zamiane Pythonowskiego None na htmlowy ciąg: <puste>
def hNone2EmptyStr(val):
	if val is None:
		return ""#("%spuste%s" % (HTML_LT, HTML_GT) )
	else:
		return val
		
# zapis danych do pliku .html
# @fName - nazwa pliku
# @header - lista kolumn
# @data - dane z tabeli
def export_to_html(fName, header, data):
	# DEBUG: print(data)
	print("Eksport danych do pliku .html... ")
	result = True
	try:
		r_count = 1
		f = open(fName, "w", encoding='utf-8')
		f.write("""<html><header>
			<META NAME="Generator" CONTENT="MySQLTablesExport.py\n">
			<META NAME="Author" CONTENT="OuNiS"\n>
			<META HTTP-EQUIV="content-type" CONTENT="text/html; charset=windows-1250">\n
			</header>\n""")
		f.write("""<body>\n
			<table border=\"2\">
			<tr>""")
		s_columns = "<th>lp.</th>"
		for col in header:
			s_columns += ("<th>%s</th>" % (col))
		f.write(s_columns)
		f.write("</tr>\n")
		for row in data:
			s = "<tr><td>" + str(r_count) + ".</td>"
			for i in range(0, len(row)):
				s += "<td>" + str(hNone2EmptyStr(row[i]))+"</td>"
			s += "</tr>"
			f.write(s+"\n")
			#print("%d" % (r_count), end=' ')
			#if r_count % 10 == 0:
				#print("\n")
			r_count += 1
			
		f.write("</table>")
		f.write("</body>\n")
		f.write("</html>\n")
		f.flush()
		f.close()
	except Exception as e:
		eM = [i for i in e.args]
		print("Błąd podczas generowania pliku .html: %s" % (eM))
		result = False
	else:
		print("Gotowe")
	return result
#end of def export2Html()



def getExceptionMessage(e):
	result = ""
	if (e is not None):
		result = str(e.__class__)
		result = result[len("<class '"):-len("'>")] + "\n" + str([i for i in (e.args)])
		
	
	return result
	

def saveCFG(fileName):
	try:
		f = open(fileName, "w")
		for x in range(len(SERVER_LIST)):
			f.write(CONN_DATA_SAVE_FORMAT % (SERVER_LIST[x]['Address'], SERVER_LIST[x]['User']))
		f.flush()
		f.close()
	except Exception as e:
		s = str(e.__class__)
		s = s[len("<class '"):-len("'>")] #żeby to jakoś wyglądało
		print(getExceptionMessage(e))
		#print(s)
		#print(str([i for i in (e.args)]))
	#except IOError as Argument:
		#print()
		#print(str([i for i in (Argument.args)]))
	#except TypeError as Argument:
		#print(str([i for i in (Argument.args)]))
	#except ValueError as e:
		#print(str([i for i in (e.args)]))
	
def loadCFG(fileName):
	result = []
	lines = []
	try:
		file = open(fileName, "r")
		#data = f.read()
		
		# s = ""
		#stara wersja zapełniania tablicy liniami z pobranych danych z pliku
		# separator linii \n w stałek END_LINE_SEP
		##print(lines)
		#if (len(data) > 0):
			#for i in range(len(data)):
				#if(data[i] == END_LINE_SEP):
					#result.append(s)
					#s = ""
				#else:
					#s += data[i]	
		
		for line in file:
			lines.append(line.splitlines()[0]) # wywalić niepotrzebny w tablicy znak końca wiersza					
		#lines = data.splitlines()
		
		user = ""
		address = ""
		port = 0
		
		for line in lines:
			pairs = line.split(DICT_ITEMS_SEP)
			
			for pair in pairs:
				keyvalues = pair.split(DICT_VALUES_SEP)
				key = keyvalues[0]
				value = keyvalues[1]
				
				if key.lower() == 'address':
					address = value
				if key.lower() == 'user':
					user = value
				if key.lower() == 'port':
					port = int(value)
			result.append({'Address':address, 'User':user, 'Port':port})

		file.close()
	except Exception as e:
		print(getExceptionMessage(e))
		#print(str(e.__class__)[len("<class '"):-len("'>")] + "\n" + str([i for i in (e.args)]))
	
	return result


def main(args):

	menu = []
	
	servers = loadCFG("conndata.cfg")
	
	
	for item in servers:
		p = ""
		if (item["Port"] != 0):
			p = str(item["Port"])
			
		menu.append(item["Address"] + " " + p)
	
	answ = ChooseOption(menu, "Lista serwerów: ")
	if (answ ==None or answ == -1):
		return 0
	
	server = servers[answ]
	
	DEF_SERVER['Address'] = server['Address']
	if (not server['Port'] is None):
		DEF_SERVER['Port'] = server['Port']
	else:
		DEF_SERVER['Port'] = DEF_PORT
		
	User['Name'] = server['User']
	
	result = 0
	print("Wybrany serwer: %s " % DEF_SERVER['Address'])
	#User['Name'] = DEF_SERVER['User']
	DBConnect = Connect(prepareHost(server), User, "", DEF_SERVER['Port']) # podłączenie do serwera bez wskazanej bazy danych
	print("User: %s" % (User['Name']))
	if DBConnect is not None:
		#print("Połączenie powiodło się!")
		DBases = GetDatabases(DBConnect)
		# DEBUG: print(DBases)
		if DBases is not None:
			menu = DBases
			answ = ChooseOption(menu, "Lista baz danych na serwerze: " + DEF_SERVER['Address'])
			if (answ is not None and answ != -1):
				#pass
				# DEBUG: print(menu[answ])
				seldb = DBases[answ]
				DBConnect = Connect(DEF_SERVER['Address'], User, DBases[answ], DEF_SERVER['Port']) # ponowne połączenie z wybraną bazą DBases[answ]
				if (DBConnect is not None):
					Tables = GetTables(DBConnect)
					if Tables is not None:
						menu = Tables
						answ = ChooseOption(menu, "Lista tabel w bazie: " + DBases[answ], DBConnect)
						if (answ is not None and answ != -1):
							#print("Wybrana tabela: %s" % menu[answ])
							menu = [EXP_TXT, EXP_HTML]
							seltab = Tables[answ]
							answ = ChooseOption(menu, "Generuj export dla tabeli {0}: ".format(seltab.upper()))
							# DEBUG print("answ - %d"  % answ)
							if (answ is not None and answ != -1):
								print(menu[answ])
								vLimit = 0

								cursor = GenExpSQL(DBConnect, SQL_EXP_COUNT, seltab, None )
								if not cursor is None:
									tab_rows = getTableRows(cursor)
								row = tab_rows[0];


								buff = input("Wprowadź limit z zakresu 1 - %d dla exportowanych danych dla tabeli: %s ([ENTER] - bez ograniczeń): " % (  row[0], seltab.upper()))
								if (buff.isnumeric()):
									vLimit = int(buff)
								else:
									vLimit = -1
								
								cursor = GenExpSQL(DBConnect, SQL_EXP_QRY, seltab, vLimit)
								if cursor is None:
									print("Brak wyniku zapytania")
								#FieldNames = [i[0] for i in Cursor.description]
								#print(FieldNames)
								tab_header = None
								tab_header = getTableHeader(cursor)
								# answ = ChooseOption(tab_header, "Wybierz pole do sortowania: ")
								#
								# if (not answ == None and answ > -1):
								# 	print(f'{answ} - numer kolumny do sortowania, {tab_header[answ]} - nazwa klumny do sortowania')


								tab_rows = None
								tab_rows = getTableRows(cursor)
								exp_succ = False
								if (tab_header is not None and tab_rows is  not None):
									exp_file = ""
									if menu[answ] == EXP_TXT:
										exp_file = createFileName(seldb+"_"+seltab, EXT_TXT)
										exp_succ = export_to_txt(exp_file, tab_header, tab_rows)
									if menu[answ] == EXP_HTML:
										exp_file = createFileName(seldb+"_"+seltab, EXT_HTML)
										exp_succ = export_to_html(exp_file, tab_header, tab_rows)
									if (exp_succ is not False):
										print("Dane wyeksportowane z tabeli: %s do pliku: %s" % (seltab.upper(), exp_file))
										print("Zapisanych wierszy: %d" % (len(tab_rows)))
									#print(len(rows))
									else:
										result = 10 # Export error
									pass
								else:
									print("Błąd podczas pobierania nagłówka i zawartości tabeli")
							elif answ is None:
								print("Nie wybrano akcji")
							else:
								print("Wyjscie a")
						elif answ is None:
							print("Nie wybrano tabeli")
						else:
							print("Wyjście t")
					else:
						print("Problem z załadowaniem listy tabel")
				else:
					print("Cuś nie tak...")
			elif answ is None:
				print("Nie wybrano bazy")
			else:
				print("Wyjście b")
		else:
			print("Problem z załadowaniem listy baz")
		
		
	if DBConnect is not None:
		DBConnect.close()
	return result




if __name__ == '__main__':
	import sys
	try:
		sys.exit(main(sys.argv))
	except KeyboardInterrupt as e: # wywołane Ctl-C
		print(str(e.__class__) + " Przerwane przez klienta") 
