import sqlite3
import csv
import json
import codecs
import sys
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
data_choc = []
conn = sqlite3.connect('choc.db')
cur = conn.cursor()

def create_bars_db():
    # Drop tables
    statement = '''
    DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)


    with open("flavors_of_cacao_cleaned.csv") as BARSCSV:
        csvReader = csv.reader(BARSCSV)
        for row in csvReader:
            if "Company" != row[0]:
                row[4] = row[4].rstrip("%")
                row[4] = float(row[4])*0.01
                row[4] = round(row[4],2)
                row[6] = round(float(row[6]),2)

                #print(type(row[4]))
                print(row[6])
            data_choc.append(row)


        #data_choc = data_choc[1:]
        #print(data_choc[0])


    statement = '''
            CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocationId' INTEGER NOT NULL,
                'Rating' REAL NOT NULL,
                'BeanType' TEXT NOT NULL,
                'BroadBeanOriginId' INTEGER NOT NULL

            );
        '''

    cur.execute(statement)

    for inst in data_choc[1:]:
        insertion = (None, inst[0], inst[1], inst[2],inst[3], inst[4], inst[5],inst[6], inst[7], inst[8])
        #print(insertion)
        statement = 'INSERT INTO "Bars"'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement,insertion)

    conn.commit()

    pass



def create_countries_db():
    statement = '''
    DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    data_country = []
    o = open("countries.json")
    load_country = json.load(o)
    #print(load_country)

    for row in load_country:
        data_country_row = []
        #print(load_country.index(row))
        data_country_row.append(load_country.index(row)+1)
        data_country_row.append(row["alpha2Code"])
        data_country_row.append(row["alpha3Code"])
        data_country_row.append(row["name"])
        data_country_row.append(row["region"])
        data_country_row.append(row["subregion"])
        data_country_row.append(row["population"])
        data_country_row.append(row["area"])
        data_country.append(data_country_row)



    #print("this is data_counrty[4]")
    #print(data_country[4])
    statement = '''
            CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT NOT NULL,
                'Subregion' TEXT NOT NULL,
                'Population' INTEGER NOT NULL,
                'Area' REAL
            );
        '''

    cur.execute(statement)

    for inst in data_country:
        insertion = (inst[0], inst[1], inst[2], inst[3], inst[4], inst[5], inst[6],inst[7])
        print(insertion)
        statement = 'INSERT INTO "Countries"'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement,insertion)

    conn.commit()
    pass




# Part 2: Implement logic to process user commands
def process_command(command):
    return []

def bars_return(sellcountry=None,sellregion=None,ratings=None,cocoa=None,top=None, bottom=None):
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    get_bars_data = []
    statement=''' SELECT Bars.SpecificBeanBarName,Bars.Company, Bars.CompanyLocationId, Bars.Rating,Bars.CocoaPercent, Bars.BroadBeanOriginId
    '''

    if sellcountry!=None:
        statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
        statement += 'WHERE Countries.Alpha2="'+ str(sellcountry) +'"'

        if cocoa!=None:
            statement += ' ORDER BY CocoaPercent'
            if bottom != None:
                statement += ' Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)

                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
        else:
            statement += ' ORDER BY Rating '
            if bottom != None:
                statement += ' Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)

                return(get_bars_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
    elif sellregion!=None:
        statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
        statement += 'WHERE Countries.Region="'+ str(sellregion) +'"'

        if cocoa!=None:
            statement += ' ORDER BY CocoaPercent'
            if bottom != None:
                statement += ' Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
        else:
            statement += ' ORDER BY Rating'
            if bottom != None:
                statement += ' Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)


                return(get_bars_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                #print(get_bars_data)
                return(get_bars_data)


    else:
        statement += ' From Bars'
        if cocoa!=None:
            statement += ' ORDER BY CocoaPercent'
            if bottom != None:
                statement += ' Limit "' + str(bottom+1) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                del get_bars_data[0]
                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top+1) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)

                del get_bars_data[0]
                return(get_bars_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                del get_bars_data[0]
                return(get_bars_data)
        else:
            statement += ' ORDER BY Rating'
            if bottom != None:
                statement += ' Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                del get_bars_data[0]
                return(get_bars_data)
            elif top !=None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)
                del get_bars_data[0]
                return(get_bars_data)
            else:
                statement += ' DESC Limit 11'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_bars_data.append(row)

                del get_bars_data[0]
                return(get_bars_data)





#a=bars_return(sellcountry=None,sellregion=None,ratings=None,cocoa=None,top=None, bottom=None)
#print(a)

def companies_return(country=None,region=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None):
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    get_company_data = []

    if cocoa != None:
        statement = "SELECT Bars.Company, Bars.CompanyLocationId, AVG(Bars.CocoaPercent)"
        if country!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Alpha2="'+ str(country) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.CocoaPercent)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)

        elif region!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Region="'+ str(region) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.CocoaPercent)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
        else:
               statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
               statement += " GROUP BY Bars.Company"
               statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
               statement += ' ORDER BY AVG(Bars.CocoaPercent)'

               if bottom != None:
                   statement += '  Limit "' + str(bottom) + '" '
                   print(statement)
                   cur.execute(statement)
                   for row in cur:
                       get_company_data.append(row)
                   return(get_company_data)
               elif top != None:
                   statement += ' DESC Limit "' + str(top) + '" '
                   print(statement)
                   cur.execute(statement)
                   for row in cur:
                       get_company_data.append(row)
                   return(get_company_data)
               else:
                   statement += ' DESC Limit 10'
                   print(statement)
                   cur.execute(statement)
                   for row in cur:
                       get_company_data.append(row)
                   return(get_company_data)


    elif bars_sold != None:
        statement = "SELECT Bars.Company, Bars.CompanyLocationId, COUNT(Bars.SpecificBeanBarName)"
        if country!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Alpha2="'+ str(country) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY COUNT(Bars.SpecificBeanBarName)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)

        if region!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Region="'+ str(country) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY COUNT(Bars.SpecificBeanBarName)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
        else:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY COUNT(Bars.SpecificBeanBarName)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)

    else:
        statement = "SELECT Bars.Company,Bars.CompanyLocationId,AVG(Bars.Rating)"
        if country!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Alpha2="'+ str(country) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.Rating)'
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)

        if region!=None:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += " GROUP BY Bars.Company"
            statement += ' HAVING Countries.Region="'+ str(country) +'"'
            statement += ' AND COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.Rating) '
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
        else:
            statement += ''' FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName '''
            statement += "GROUP BY Bars.Company"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.Rating) '
            if bottom != None:
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            elif top != None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_company_data.append(row)
                return(get_company_data)


#b=companies_return(country=None,region=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None)
#print(b)


def countries_return(region=None,sellers=None,sources=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None):
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    get_company_data = []
    if cocoa!=None:
        statement = "SELECT Countries.EnglishName,Countries.Region,AVG(Bars.CocoaPercent)"
        statement += " FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName"
        if region!=None:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY AVG(Bars.CocoaPercent) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY AVG(Bars.CocoaPercent) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

        else:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY AVG(Bars.CocoaPercent) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY AVG(Bars.CocoaPercent) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)




    elif bars_sold!=None:
        statement = "SELECT Countries.EnglishName , Countries.Region, COUNT(Bars.SpecificBeanBarName)"
        statement += " FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName"
        if region!=None:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY COUNT(Bars.SpecificBeanBarName) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY COUNT(Bars.SpecificBeanBarName) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

        else:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY COUNT(Bars.SpecificBeanBarName) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY COUNT(Bars.SpecificBeanBarName) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)


    else:
        statement = "SELECT Countries.EnglishName , Countries.Region,AVG(Bars.Rating)"
        statement += " FROM Bars join Countries on Bars.CompanyLocationId= Countries.EnglishName"
        if region!=None:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY AVG(Bars.Rating) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += ' AND Region="' + str(region) + '"'
                statement += " ORDER BY AVG(Bars.Rating) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)


        else:
            if sources!=None:
                statement += " GROUP BY Bars.BroadBeanOriginId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY AVG(Bars.Rating) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)

            else:
                statement += " GROUP BY Bars.CompanyLocationId"
                statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
                statement += " ORDER BY AVG(Bars.Rating) "
                if bottom != None:
                    statement += '  Limit "' + str(bottom) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                elif top != None:
                    statement += ' DESC Limit "' + str(top) + '" '
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)
                else:
                    statement += ' DESC Limit 10'
                    print(statement)
                    cur.execute(statement)
                    for row in cur:
                        get_company_data.append(row)
                    return(get_company_data)




#c=countries_return(region=None,sellers=None,sources=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None)
#print(c)

def regions_return(sellers=None,sources=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None):
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    get_regions_data = []

    if cocoa != None:
        statement = "SELECT Countries.Region, AVG(Bars.CocoaPercent)"
        statement += " FROM Bars join Countries on Bars.BroadBeanOriginId= Countries.EnglishName"
        if sources!=None:
            statement += " GROUP BY Bars.BroadBeanOriginId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += " ORDER BY AVG(Bars.CocoaPercent) "
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)
        else:
            statement += " GROUP BY Bars.CompanyLocationId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.CocoaPercent)'
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)

    elif bars_sold != None:
        statement = "SELECT Countries.Region, COUNT(Bars.SpecificBeanBarName)"
        statement += " FROM Bars join Countries on Bars.BroadBeanOriginId= Countries.EnglishName"
        if sources!=None:
            statement += " GROUP BY Bars.BroadBeanOriginId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += " ORDER BY COUNT(Bars.SpecificBeanBarName) "
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)
        else:
            statement += " GROUP BY Bars.CompanyLocationId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY COUNT(Bars.SpecificBeanBarName) '
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)

    else:
        statement = " SELECT Countries.Region, AVG(Bars.Rating) "
        statement += " FROM Bars join Countries on Bars.BroadBeanOriginId= Countries.EnglishName"
        if sources!=None:
            statement += " GROUP BY Bars.BroadBeanOriginId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += " ORDER BY AVG(Bars.Rating) "
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)
        else:
            statement += " GROUP BY Bars.CompanyLocationId"
            statement += ' HAVING COUNT(Bars.SpecificBeanBarName) > 4'
            statement += ' ORDER BY AVG(Bars.Rating) '
            if bottom != None  :
                statement += '  Limit "' + str(bottom) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            elif top!= None:
                statement += ' DESC Limit "' + str(top) + '" '
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)

                return(get_regions_data)
            else:
                statement += ' DESC Limit 10'
                print(statement)
                cur.execute(statement)
                for row in cur:
                    get_regions_data.append(row)
                return(get_regions_data)

#d=regions_return(sellers=None,sources=None,ratings=None,cocoa=None,bars_sold='bar',top=None,bottom=None)
#print(d)

#bars_return(sellcountry=None,sellregion=None,ratings=None,cocoa=None,top=None, bottom=None)
#companies_return(country=None,region=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None)
#countries_return(region=None,sellers=None,sources=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None)
#regions_return(sellers=None,sources=None,ratings=None,cocoa=None,bars_sold=None,top=None,bottom=None)




def process_command(command):
    s = command.split()
    list_bar=[None,None,None,None,None,None]
    list_companies=[None,None,None,None,None,None,None]
    list_countries=[None,None,None,None,None,None,None,None]
    list_regions=[None,None,None,None,None,None,None]
    if "bars" in s:
        print("using bars")
        for i in s:
            if '=' not in i:
                if "cocoa" in i:
                    list_bar[3]='cocoa'
            else:
                w=i.split('=')
                if(w[0]=='sellcountry'):
                    list_bar[0]=w[1]
                elif(w[0]=='sellregion'):
                    list_bar[1]=w[1]
                elif(w[0]=='top'):
                    list_bar[4]=w[1]
                elif(w[0]=='bottom'):
                    list_bar[5]=w[1]

        return(bars_return(list_bar[0],list_bar[1],list_bar[2],list_bar[3],list_bar[4],list_bar[5]))

    elif "companies" in s:
        print("using companies")
        for i in s:
            if '=' not in i:
                if "cocoa" in i:
                    list_companies[3]='cocoa'
                if "bars_sold" in i:
                    list_companies[4]='cocoa'
            else:
                w=i.split('=')
                if(w[0]=='country'):
                    list_companies[0]=w[1]
                elif(w[0]=='region'):
                    list_companies[1]=w[1]
                elif(w[0]=='top'):
                    list_companies[5]=w[1]
                elif(w[0]=='bottom'):
                    list_companies[6]=w[1]

        return(companies_return(list_companies[0],list_companies[1],list_companies[2],list_companies[3],list_companies[4],list_companies[5],list_companies[6]))

    elif "countries" in s:
        print("using countries")
        for i in s:
            if '=' not in i:
                if "sources" in i:
                    list_countries[2]='sources'
                if "cocoa" in i:
                    list_countries[4]='cocoa'
                if "bars_sold" in i:
                    list_bar[5]='cocoa'
            else:
                w=i.split('=')
                if(w[0]=='region'):
                    list_countries[0]=w[1]
                elif(w[0]=='top'):
                    list_countries[6]=w[1]
                elif(w[0]=='bottom'):
                    list_countries[7]=w[1]
        return(countries_return(list_countries[0],list_countries[1],list_countries[2],list_countries[3],list_countries[4],list_countries[5],list_countries[6],list_countries[7]))

    elif "regions" in s:
        print("using region")
        for i in s:
            if '=' not in i:
                if "cocoa" in i:
                    list_regions[3]='cocoa'
                if "bars_sold" in i:
                    list_regions[4]='cocoa'
            else:
                w=i.split('=')
                if(w[0]=='top'):
                    list_regions[5]=w[1]
                elif(w[0]=='bottom'):
                    list_regions[6]=w[1]


        return(regions_return(list_regions[0],list_regions[1],list_regions[2],list_regions[3],list_regions[4],list_regions[5],list_regions[6]))



def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    command = ''
    while(True):
        command = str(input('Enter a command: '))
        if command == 'help':
            print(help_text)
            continue
        elif command == 'Exit':
            break
        else:
            print(command)
            content=process_command(command)
            for i in content:
                print(i)






# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
