
# import shutil
from os import listdir
from os.path import isfile, join
import pandas as pd
"""This code will compile data and create a CSV with all the data from the source.
The source files do not include a year columns so one is added."""
#data source: https://www.ssa.gov/oact/babynames/limits.html



mypath = "your file path to unzipped source data here"
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

print onlyfiles
print "START"

compiled_file_name = "Names_Nationally.txt"

def concat_files(mypath, onlyfiles, outfilename):
	final_df = pd.DataFrame()
# 	with open(outfilename, 'wb') as outfile:
	for filename in onlyfiles:
		is_text_file = filename.find(".txt") >= 0
		print str(filename), " text file?", is_text_file
		if filename == outfilename or\
			is_text_file == False:
			# don't want to copy the output or pdfs into the output
			year = ""
			continue
			o_df['year'] = year
		elif filename <> outfilename and\
			filename.find(".txt") <> -1:
			with open(mypath + "/" + filename, 'rb') as readfile:
				year = str(filename)[3:7]
				o_df = pd.read_csv(readfile, sep=",", header=None)
				o_df.columns = ["Name", "Sex", "Occurances"]
				o_df['Year'] = year
				final_df = pd.concat([final_df, o_df])
# 					print "----", o_df.head()
				print year
		else:
			continue
		print year, filename
	final_df.to_csv(outfilename, index=False)
concat_files(mypath, onlyfiles, compiled_file_name)
print "new file:", compiled_file_name, 
# the output file will be in the same location as this .py file
print "END"