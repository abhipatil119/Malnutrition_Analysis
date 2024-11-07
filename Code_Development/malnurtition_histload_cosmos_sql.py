# -*- coding: utf-8 -*-
"""Malnurtition_Histload_sql.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1k1ADAab_7QxGCYvQqkqwFfDPz9sCXGZa
"""

import os
import openpyxl
import glob
# Get the path of the active workbook
filepath = os.path.abspath(os.getcwd())

filename_list = glob.glob('Raw/JME_Country_Estimates*.xlsx')

# Load the first file in the list (assuming there is at least one file)
wb = openpyxl.load_workbook(filename=filename_list[0])
wb.min_row = 1
wb.min_column = 1

# Loop through each worksheet in the current workbook
for ws in wb.worksheets:
    # Create a new workbook with the same name as the worksheet
    new_wb = openpyxl.Workbook()
    new_ws = new_wb.active
    new_ws.title = ws.title
    
    # Copy the values and formatting from the original worksheet to the new worksheet
    for row in ws.rows:
        for cell in row:
            new_ws[cell.coordinate].value = cell.value
    # Save the new worksheet as a separate file with the same name as the worksheet
    new_wb.save(filepath + '/Temp/' + ws.title + '.xlsx')
    
# Close the original workbook
wb.close()

import pandas as pd
stuntprop= pd.read_excel("Temp/Stunting Proportion (Model).xlsx")
#Get stunt propoption data
load_year=stuntprop.columns[6:]
stuntprop_final = pd.DataFrame()
for year in load_year:
  tstuntprop = pd.DataFrame()
  tstuntprop = stuntprop[[ 'ISO code','Country and areas','Estimate',year]].copy()
  tstuntprop['Year']=year[:4]
  tstuntprop.rename(columns={"Country and areas": "Country", "ISO code": "ISO_code",year:"stunting_proportion"}, inplace=True)
  tstuntprop= tstuntprop.loc[ tstuntprop['Estimate'] == 'Point Estimate'].reset_index().drop(["Estimate","index"], axis = 1)
  stuntprop_final=pd.concat([stuntprop_final, tstuntprop], ignore_index=True)
stuntprop_final

#Get stunt affected data
stuntaffected= pd.read_excel("Temp/Stunting Numb Affected(Model).xlsx")
load_year=stuntaffected.columns[6:]
stuntaffected_final = pd.DataFrame()
for year in load_year:
  #print(type(year))
  tstuntaffected = pd.DataFrame()
  tstuntaffected = stuntaffected[[ 'ISO code','Country and areas','Estimate',year]].copy()
  tstuntaffected['Year']=year[:4]
  tstuntaffected.rename(columns={"Country and areas": "Country", "ISO code": "ISO_code",year:"stunting_affected"}, inplace=True)
  tstuntaffected= tstuntaffected.loc[ tstuntaffected['Estimate'] == 'Point Estimate'].reset_index().drop(["Estimate","index"], axis = 1)
  stuntaffected_final=pd.concat([stuntaffected_final, tstuntaffected], ignore_index=True)
stuntaffected_final

#Get overweight affected data
overwgtaffected= pd.read_excel("Temp/Overweight Numb Affected(Model).xlsx")
load_year=overwgtaffected.columns[6:]
overwgtaffected_final = pd.DataFrame()
for year in load_year:
  toverwgtaffected = pd.DataFrame()
  toverwgtaffected = overwgtaffected[[ 'ISO code','Country and areas','Estimate',year]].copy()
  toverwgtaffected['Year']=year[:4]
  toverwgtaffected.rename(columns={"Country and areas": "Country", "ISO code": "ISO_code",year:"overweight_affected"}, inplace=True)
  toverwgtaffected= toverwgtaffected.loc[ toverwgtaffected['Estimate'] == 'Point Estimate'].reset_index().drop(["Estimate","index"], axis = 1)
  overwgtaffected_final=pd.concat([overwgtaffected_final, toverwgtaffected], ignore_index=True)
overwgtaffected_final

#Get overweight propoption data
overweightprop= pd.read_excel("Temp/Overweight Proportion (Model).xlsx")
load_year=overweightprop.columns[6:]
overweightprop_final = pd.DataFrame()
for year in load_year:
  toverweightprop = pd.DataFrame()
  toverweightprop = overweightprop[[ 'ISO code','Country and areas','Estimate',year]].copy()
  toverweightprop['Year']=year[:4]
  toverweightprop.rename(columns={"Country and areas": "Country", "ISO code": "ISO_code",year:"overweight_proportion"}, inplace=True)
  toverweightprop= toverweightprop.loc[ toverweightprop['Estimate'] == 'Point Estimate'].reset_index().drop(["Estimate","index"], axis = 1)
  overweightprop_final=pd.concat([overweightprop_final, toverweightprop], ignore_index=True)
overweightprop_final

#Merge all dataframes to get Malnutrition_impact_hist
Final_Summary =overweightprop_final.merge(overwgtaffected_final).merge(stuntprop_final).merge(stuntaffected_final)
Final_Summary=Final_Summary[['ISO_code','Country','Year','overweight_proportion','overweight_affected','stunting_proportion','stunting_affected']]
Final_Summary.head()

Final_Summary.tail()

Final_Summary.to_excel("source/Malnutrition_impact_hist.xlsx", index=False)

survey= pd.read_excel("Temp/Survey Estimates.xlsx")

survey.head()

#Get country_details data
country_details= survey[['ISO code','Country and areas','United Nations Region','WHO Region','UNICEF Region']].copy()

country_details.rename(columns={"ISO code":"ISO_code","Country and areas": "Country", "United Nations Region": "UN_region","WHO Region":"WHO_region","UNICEF Region":"UNICEF_region"}, inplace=True)

country_details=country_details.drop_duplicates().reset_index().drop(["index"], axis = 1)

country_details.to_excel("source/country_details.xlsx", index=False)

#Get income_classification data
income_classification= survey[['ISO code','World Bank Income Classification','LDC','LIFD','LLDC or SIDS']].copy()
	

income_classification.rename(columns={"ISO code":"ISO_code","World Bank Income Classification": "wb_income_class", "LLDC or SIDS":"LLDC_SIDS"}, inplace=True)

income_classification=income_classification.drop_duplicates().reset_index().drop(["index"], axis = 1)

income_classification.to_excel("source/income_classification.xlsx", index=False)

#Delete All temporary Files
dir = 'Temp/'
for f in os.listdir(dir):
    os.remove(os.path.join(dir, f))