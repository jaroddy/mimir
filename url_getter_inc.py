# -*- coding: utf-8 -*-
"""
Created on Wed Oct 13 10:03:40 2021

@author: jarod
"""

import os, sys
import requests
import urllib.request
from bs4 import BeautifulSoup
import re
import pandas as pd
import gzip




#List GEO Accession Numbers

accessions = ["GSE146937", "GSE146938", "GSE146939",
             "GSE146940", "GSE146941", "GSE146942",
             "GSE142736", "GSE142738", "GSE142739",
             "GSE140325", "GSE132290", "GSE132291",
             "GSE132292", "GSE132293", "GSE132159",
             "GSE132160", "GSE132162", "GSE132163",
             "GSE117313", "GSE117323", "GSE117324",
             "GSE115959", "GSE115960", "GSE115961",
             "GSE58907",  "GSE55579",  "GSE53322",
             "GSE53323",  "GSE53324",  "GSE29983",
             "GSE26189",  "GSE11298",  "GSE8962",
             "GSE8222",   "GSE8177",   "GSE7411",
             "GSE7168"]


#Set Global variables and counters

directory = os.listdir()

file_path = os.getcwd()

#Counters are used to ensure one accession, title and description is used per sample
 
sample_acc_counter = 0

title_counter = 0

desc_counter = 0

#acc_table is the table we will fill out that has the same format as the example file for the mimir database

acc_table = pd.DataFrame(index=range(2000), columns=['Submission_Date','accession', 'GEO_SID','File_Path','File','URL','Sample_Description','Sample_Title','DS','AG'])

#count tracks when the beginning and end of a datapoint with "" has been determined

count = 0

def savePage(url, pagefilename='page'): #
    def soupfindnSave(pagefolder, tag2find='img', inner='src'):
        """saves on specified `pagefolder` all tag2find objects"""
        #if not os.path.exists(pagefolder): # create only once
            #os.mkdir(pagefolder)
        for res in soup.findAll(tag2find):   # images, css, etc..
            try:         
                if not res.has_attr(inner): # check if inner tag (file object) exists
                    continue # may or may not exist
                filename = re.sub('\W+', '', os.path.basename(res[inner])) # clean special chars
                fileurl = urllib.request.urljoin(url, res.get(inner))
                filepath = os.path.join(pagefolder, filename)
                # rename html ref so can move html and folder of files anywhere
                res[inner] = os.path.join(os.path.basename(pagefolder), filename)
                if not os.path.isfile(filepath): # was not downloaded
                    with open(filepath, 'wb') as file:
                        filebin = session.get(fileurl)
                        file.write(filebin.content)
            except Exception as exc:
                print(exc, file=sys.stderr)
        return soup
    
    session = requests.Session()
    #... whatever other requests config you need here
    response = session.get(url)
    soup = BeautifulSoup(response.text, features="lxml")
    pagefolder = pagefilename+'_files' # page contents
    soup = soupfindnSave(pagefolder, 'img', 'src')
    soup = soupfindnSave(pagefolder, 'link', 'href')
    soup = soupfindnSave(pagefolder, 'script', 'src')
    with open(pagefilename+'.html', 'wb') as file:
        file.write(soup.prettify('utf-8'))
    return soup

def pull_raw_data(accessions):
    #For numbers in accessions and based on the length of the accession number, 
    #navigate to a website and using link.get('href') get the exact address of the compressed submission file
    
    for acc in accessions:
    
        if len(acc) == 7:
        
            soup = savePage('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:4] + 'nnn/' + acc + '/matrix/', acc) #Creates html object from which link can be referenced

            for link in soup.find_all('a'):
                temp = link.get('href')
                if 'txt.gz' in temp:
                    urllib.request.urlretrieve('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:4] + 'nnn/' + acc + '/matrix/' + link.get('href'), link.get('href')) #Downloads files at the url
                    
            
        elif len(acc) == 8:
        
            soup = savePage('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:5] + 'nnn/' + acc + '/matrix/', acc)

            for link in soup.find_all('a'):
                temp = link.get('href')
                if 'txt.gz' in temp:
                    urllib.request.urlretrieve('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:5] + 'nnn/' + acc + '/matrix/' + link.get('href'), link.get('href'))
                    
    
        elif len(acc) == 9:
        
            soup = savePage('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:6] + 'nnn/' + acc + '/matrix/', acc)

            for link in soup.find_all('a'):
                temp = link.get('href')
                if 'txt.gz' in temp:
                    urllib.request.urlretrieve('https://ftp.ncbi.nlm.nih.gov/geo/series/' + acc[0:6] + 'nnn/' + acc + '/matrix/' + link.get('href'), link.get('href'))
                                  
def find_acc_date_title(accessions, acc_table, title_counter, sample_acc_counter, directory):

    for x in range(len(directory)): #for each file in the local directory
            
            file = directory[x] #temp file
            
            for acc in accessions:
                
                if (acc in file) and (file[len(file)-3:len(file)] == '.gz'): #if file contains .gz at the end
               
                    with gzip.open(file, 'rb') as f: #unzip file temporarily
                    
                        lines = f.readlines() #create a list of lines from the unzipped text file
                    
                        for z in range(len(lines)): #for each line of the txt file
                        
                            if 'Series_geo_accession' in str(lines[z]): #look for line that has 'Series_geo_accession
                            
                                acc_line = str(lines[z]) #temp variable holding the acc_number
                            
                                count = 0
                            
                                h = 0
                            
                                for h in range(len(acc_line)):
                                
                                    if acc_line[h] == '"' and count == 0: #find beginning of the data
                                    
                                        begin = h + 1
                                    
                                        count = 1
                                    
                                    elif acc_line[h] == '"' and count == 1: #find end of the data
                                    
                                        end = h
                                    
                                        series_acc = acc_line[begin:end] 
                            
                            elif 'Series_submission_date' in str(lines[z]): #look for line containing txt
                            
                                desc_line = str(lines[z]) #temp variable holding date
                                
                                count = 0
                            
                                h = 0
                            
                                for h in range(len(desc_line)):
                                
                                     if desc_line[h] == '"' and count == 0:
                                
                                        begin = h + 1
                                    
                                        count = 1
                                    
                                     elif desc_line[h] == '"' and count == 1:
                                    
                                        end = h
                                        
                                        sub_date = desc_line[begin:end]
                                    
                    
                            elif 'Sample_geo_accession' in str(lines[z]):
                            
                                samp_line = str(lines[z])
                            
                                count = 0
                            
                                h = 0
                            
                                for h in range(len(samp_line)):
                                
                                    if samp_line[h] == '"' and count == 0:
                                     
                                        begin = h + 1
                                    
                                        count = 1
                                    
                                    elif samp_line[h] == '"' and count == 1:
                                    
                                        end = h
                                    
                                        count = 0
                                    
                                        acc_table.accession[sample_acc_counter] = series_acc
                                    
                                        sample_acc = samp_line[begin:end]
                                    
                                        acc_table.GEO_SID[sample_acc_counter] = sample_acc
                                    
                                        acc_table.Submission_Date[sample_acc_counter] = sub_date
                                     
                                        acc_table.File_Path[sample_acc_counter] = str(file_path + '\\' + file)
                                    
                                        acc_table.File[sample_acc_counter] = str(file)
                                    
                                        sample_acc_counter = sample_acc_counter + 1
                                    
                            elif ('Sample_title' in str(lines[z])):
                            
                                desc_line = str(lines[z])
                                
                                count = 0
                            
                                h = 0
                             
                                for h in range(len(desc_line)):
                                
                                    if desc_line[h] == '"' and count == 0:
                                
                                        begin = h + 1
                                    
                                        count = 1
                                    
                                    elif desc_line[h] == '"' and count == 1:
                                    
                                        end = h
                                    
                                        count = 0
                                        
                                        sample_desc = desc_line[begin:end]
                                    
                                        acc_table.Sample_Title[title_counter] = sample_desc
                                    
                                        title_counter = title_counter + 1
    return acc_table

def find_sample_desc(acc_table, desc_counter,directory):
       
       for x in range(len(directory)):
            
            file = directory[x]
            
            for acc in accessions:
                
                if (acc in file) and (file[len(file)-3:len(file)] == '.gz'):
               
                    with gzip.open(file, 'rb') as f:
                    
                        lines = f.readlines()
                
                        desc_line = ""
                    
                        for z in range(len(lines)):
                            
                             desc_line = ""
                                          
                             if ('Sample_description' in str(lines[z])):
                                    
                                 desc_line = str(lines[z])
                                        
                                 count = 0
                                 
                                 h = 0
                                    
                                 for h in range(len(desc_line)):
                                        
                                     if desc_line[h] == '"' and count == 0:
                                        
                                         begin = h + 1
                                            
                                         count = 1
                                            
                                     elif desc_line[h] == '"' and count == 1:
                                            
                                         end = h
                                            
                                         count = 0
                                        
                                         sample_desc = desc_line[begin:end]
                                         
                                         if sample_desc == "": 
                                            
                                             acc_table.Sample_Description[desc_counter] = 'nan'
                                             
                                         else:
                                             
                                             acc_table.Sample_Description[desc_counter] = sample_desc
                                             
                                         desc_counter = desc_counter + 1
                             
       return acc_table                         

def fill_urls(acc_table):
             
   for samp in range(len(acc_table)): 
    
       temp_acc = str(acc_table.accession[samp])
    
       if len(temp_acc) == 7:
    
           acc_table.URL[samp] = 'https://ftp.ncbi.nlm.nih.gov/geo/series/' + temp_acc[0:4] + 'nnn/' + temp_acc + '/matrix/' + acc_table.File[samp]
        
       elif len(temp_acc) == 8:
        
           acc_table.URL[samp] = 'https://ftp.ncbi.nlm.nih.gov/geo/series/' + temp_acc[0:5] + 'nnn/' + temp_acc + '/matrix/' + acc_table.File[samp]
        
       elif len(temp_acc) == 9:
        
           acc_table.URL[samp] = 'https://ftp.ncbi.nlm.nih.gov/geo/series/' + temp_acc[0:6] + 'nnn/' + temp_acc + '/matrix/' + acc_table.File[samp]
           
   return(acc_table)

def find_AG_DS(acc_table):
    
    for samp in range(len(acc_table)):
    
        temp_desc = str(acc_table.Sample_Description[samp])
        
        temp_title = str(acc_table.Sample_Title[samp])
        
        temp_DS = 'nan'
        
        temp_AG = 'nan'
        
        count = 0
    
        for pos in range(len(temp_desc)):
            
            if (temp_desc[pos:pos+2] == 'DS') and (temp_desc[pos+3:pos+7].isnumeric() == True) and (count == 0):
                
                acc_table.DS[samp] = temp_desc[pos:pos+7]
                
                temp_DS = temp_desc[pos:pos+7]
                
                count = 1
                
            elif (temp_desc[pos:pos+2] == 'DS') and (temp_desc[pos+3:pos+6].isnumeric() == True) and (count == 0):
                
                acc_table.DS[samp] = temp_desc[pos:pos+6]
                
                temp_DS = temp_desc[pos:pos+6]
                
                count = 1
                
            elif (temp_desc[pos:pos+2] == 'DS') and (temp_desc[pos+3:pos+7].isnumeric() == True) and (count == 1) and (temp_DS != 'nan'):
                
                acc_table.DS[samp] = temp_DS + ',' + temp_desc[pos:pos+7]
                
            elif (temp_desc[pos:pos+2] == 'DS') and (temp_desc[pos+3:pos+6].isnumeric() == True) and (count == 1) and (temp_DS != 'nan'):
                
                acc_table.DS[samp] = temp_DS + ',' + temp_desc[pos:pos+6]
                
                
                
            elif (temp_desc[pos:pos+2] == 'AG') and (temp_desc[pos+3:pos+7].isnumeric() == True) and (count == 0):
                
                acc_table.AG[samp] = temp_desc[pos:pos+7]
                
                temp_AG = temp_desc[pos:pos+7]
                
                count = 1
                
            elif (temp_desc[pos:pos+2] == 'AG') and (temp_desc[pos+3:pos+6].isnumeric() == True) and (count == 0):
                
                acc_table.AG[samp] = temp_desc[pos:pos+6]
                
                temp_AG = temp_desc[pos:pos+6]
                
                count = 1
                
            elif (temp_desc[pos:pos+2] == 'AG') and (temp_desc[pos+3:pos+7].isnumeric() == True) and (count == 1) and (temp_AG != 'nan'):
                
                acc_table.AG[samp] = temp_AG + ',' + temp_desc[pos:pos+7]
                
            elif (temp_desc[pos:pos+2] == 'AG') and (temp_desc[pos+3:pos+6].isnumeric() == True) and (count == 1) and (temp_AG != 'nan'):
                
                acc_table.AG[samp] = temp_AG + ',' + temp_desc[pos:pos+6]
                
                
                
        for pos in range(len(temp_title)):
            
            if (temp_title[pos:pos+2] == 'DS') and (temp_title[pos+3:pos+7].isnumeric() == True) and (count == 0):
                
                acc_table.DS[samp] = temp_title[pos:pos+7]
                
                temp_DS = temp_title[pos:pos+7]
                
                count = 1
                
            elif (temp_title[pos:pos+2] == 'DS') and (temp_title[pos+3:pos+6].isnumeric() == True) and (count == 0):
                
                acc_table.DS[samp] = temp_title[pos:pos+6]
                
                temp_DS = temp_title[pos:pos+6]
                
                count = 1
                
            elif (temp_title[pos:pos+2] == 'DS') and (temp_title[pos+3:pos+7].isnumeric() == True) and (count == 1) and (temp_DS != 'nan'):
                
                acc_table.DS[samp] = temp_DS + ',' + temp_title[pos:pos+7]
                
            elif (temp_title[pos:pos+2] == 'DS') and (temp_title[pos+3:pos+6].isnumeric() == True) and (count == 1) and (temp_DS != 'nan'):
                
                acc_table.DS[samp] = temp_DS + ',' + temp_title[pos:pos+6]
                
                
                
            elif (temp_title[pos:pos+2] == 'AG') and (temp_title[pos+3:pos+7].isnumeric() == True) and (count == 0):
                
                acc_table.AG[samp] = temp_title[pos:pos+7]
                
                temp_AG = temp_title[pos:pos+7]
                
                count = 1
                
            elif (temp_title[pos:pos+2] == 'AG') and (temp_title[pos+3:pos+6].isnumeric() == True) and (count == 0):
                
                acc_table.AG[samp] = temp_title[pos:pos+6]
                
                temp_AG = temp_title[pos:pos+6]
                
                count = 1
                
            elif (temp_title[pos:pos+2] == 'AG') and (temp_title[pos+3:pos+7].isnumeric() == True) and (count == 1) and (temp_AG != 'nan'):
                
                acc_table.AG[samp] = temp_AG + ',' + temp_title[pos:pos+7]
                
            elif (temp_title[pos:pos+2] == 'AG') and (temp_title[pos+3:pos+6].isnumeric() == True) and (count == 1) and (temp_AG != 'nan'):
                
                acc_table.AG[samp] = temp_AG + ',' + temp_title[pos:pos+6]
                
    return acc_table

if __name__ == "__main__":

    pull_raw_data(accessions)
    
    acc_table = find_acc_date_title(accessions, acc_table, title_counter, sample_acc_counter, directory)
    
    acc_table = find_sample_desc(acc_table, desc_counter,directory)       
    
    acc_table = fill_urls(acc_table)
    
    acc_table = find_AG_DS(acc_table)
    
    #acc_table.to_csv('GEO_accessions_data.csv', index = False)




