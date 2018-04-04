import time
start_time = time.time()

from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
pd.set_option('display.max_row', 10)
pd.set_option('display.max_columns', 15)


mypath = #change this to wherever the baby names DL was unzipped
djia_path = #change to where djia data is saved

onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

##### sample files only comment #####
# onlyfiles = onlyfiles[:1] 
run_size = 'full'

# run_size = 'sample'
if run_size == 'sample':
	file_ls = []
	start_year = 82
	sample_size = 4
	for i in range(start_year, start_year + sample_size,1):
		ind = i - start_year
		file_ls.append('yob19{0}.txt'.format(i))
	onlyfiles = file_ls

print onlyfiles
print "START"

name_set = ["name", "sex", "occurences"]
yearly_file_name = "Nm_Year_Total_national_{}.txt".format(run_size)
total_file_name = "Nm_Totals_national_{}.txt".format(run_size)
avg_file_name = "Nm_DJIA_diff_{}.txt".format(run_size)
diff_file_name = "Nm_Year_Diff_national_{}.txt".format(run_size)
djia_file_name = "DJIA_formatted.txt"
year_chg_file_name = "Nm_Year_Occ_national_change_{}.txt".format(run_size)
new_file_ls = [yearly_file_name,
	total_file_name,
	avg_file_name,
	diff_file_name,
	djia_file_name,
	year_chg_file_name
	]

def split_gen(df):
	female_df = df[df['sex'] == 'F'].copy()
	male_df = df[df['sex'] == 'M'].copy()
	return female_df, male_df

def stack_reset(df, comment=''):
	df = df.stack()
	df = df.reset_index()
	df.columns = ['name', 'year', comment+'diff']
	df = df.sort_index()
	return df

def sum_name(t_df, yr=True):
	return t_df['occurences'].rank(method='max', ascending=False).astype(int)

def sum_gender(gn_df, gndr):
	gn_df = gn_df.drop(['sex'], axis=1)
	gn_df = gn_df.groupby(['name']).sum()
	gn_df['name'] = gn_df.index
	gn_df['alltime_rank_gd'] = sum_name(gn_df)
	gn_df['sex'] = gndr
	return gn_df

# def check
def sort_max(df):
	return df.sort_values(by=['total_year_occurences'], ascending=False)

def compare_djia(df):
	with open(djia_path, 'rb') as djia_file:
		djia_df = pd.read_csv(djia_file, sep=",", header=None)
	djia_df.columns = ['yyyy_mm_dd', 'DJIA_pct_chg']
	djia_df['year'] = djia_df['yyyy_mm_dd'].str.split('-')
	djia_df['year'] = djia_df['year'].str[0]
	djia_df = djia_df.set_index('year')
	year_djia_ls = list(djia_df.index)
	year_name_ls = list(df)
	year_ls = list(set(year_name_ls) & set(year_djia_ls))
	diff_df_o = pd.DataFrame()
	diff_df = pd.DataFrame()
	meta_df = pd.DataFrame()
	djia_name_slice_df = df[year_ls]
	meta_df['N'] = len(year_ls) - djia_name_slice_df.isnull().sum(axis=1)
	offset_birthyear = 0
	for yr in year_ls:
		djia = djia_df.loc[str(int(yr) - offset_birthyear), 'DJIA_pct_chg']
		diff_df_o[yr] = (df[str(yr)] - djia).abs()
	diff_df['diff_sum'] = diff_df_o.sum(axis=1)
	diff_df['diff_var'] = diff_df_o.var(axis=1)
	diff_df = pd.concat([diff_df, meta_df], axis=1)
	diff_df['diff_avg'] = diff_df['diff_sum'] / (1.0*diff_df['N'])
	diff_df_o = stack_reset(diff_df_o)
	diff_df_o = diff_df_o.dropna(axis=0, how='all')
	return diff_df, diff_df_o, djia_df

def year_pct_change(df):
	drop_cols = ['sex', 'year_rank_ungn', 'year_rank_gd', 'occurences']
	df = df.drop(drop_cols, axis=1)
	df = df.drop_duplicates()
	df = df.reset_index(drop=True)
	df = df.sort_index()
	df = df.pivot(index='name', columns='year', values='total_year_occurences')
	past_year_cols = list(df)[1:]
	drop_year = list(df)[-1:]
	past_df = df.copy()
	past_df = past_df.drop(drop_year, axis=1)
	past_df.columns = past_year_cols
	change_pct_df = pd.DataFrame()
	for yr in past_year_cols:
		change_pct_df[yr] = 100 * (df[yr] - past_df[yr]) / past_df[yr]
	change_pct_df = change_pct_df.dropna(axis=0, how='all')
	return change_pct_df

def rank_total(o_df):
	drop_cols = ['total_year_occurences', 'year', 'year_rank_ungn', 'year_rank_gd']
	o_df = o_df.drop(drop_cols, axis=1)
	o_df = o_df.reset_index()
	o_df = o_df.drop(['index'], axis=1)
	total_df = o_df.drop(['sex'], axis=1)
	total_df = total_df.groupby(['name']).sum()
	total_df['name'] = total_df.index
	female_df, male_df = split_gen(o_df)
	female_df = sum_gender(female_df, 'F')
	male_df = sum_gender(male_df, 'M')
	gn_df = pd.concat([male_df, female_df])
	r_df = pd.DataFrame()
	r_df['alltime_total_occurences'] = total_df['occurences']
	r_df['alltime_rank_ungn'] = sum_name(total_df)
	r_df['name'] = r_df.index
	total_df = pd.merge(r_df, gn_df, on=['name'])
	total_df = total_df.rename(columns={'occurences' : 'total_occu_gn'})
	total_df = total_df.set_index('name')
	return total_df

def parse_file(df, year):
	df.columns = ['name', 'sex', 'occurences']
	o_df = df.copy()
	yr_df = o_df
	total_df = pd.DataFrame()
	#reduce totals datafram to just name and occurences
	t_df = o_df.drop(['sex'], axis=1)
	total_df = t_df.groupby(['name']).sum()
	total_df['name'] = total_df.index
	r_df = pd.DataFrame()
	#create new variable for ranking from slice
	r_df['total_year_occurences'] = total_df['occurences']
	r_df['year_rank_ungn'] = sum_name(total_df)
	yr_df = yr_df.join(r_df, on=['name'], how='outer')
	female_df, male_df = split_gen(df)
	female_df['year_rank_gd'] = sum_name(female_df)
	male_df['year_rank_gd'] = sum_name(male_df)
	gn_df = pd.concat([male_df, female_df])
	gn_df = gn_df.drop(['sex', 'occurences', 'name'], axis=1)
	yr_df = pd.concat([yr_df, gn_df], axis=1, join='outer')
	yr_df['year'] = year
	print "compile:", year
	return yr_df

def concat_files(mypath, onlyfiles, outfilenames):
	comp_df = pd.DataFrame()
	for filename in onlyfiles:
		filename = str(filename)
		is_text_file = filename.find(".txt") >= 0
		print filename, " text file?", is_text_file
		if filename in outfilenames or\
			is_text_file == False:
			# don't want to copy the output or pdfs into the output
			year = ""
			continue
		elif not filename in outfilenames and\
			filename.find(".txt") <> -1:
			with open(mypath + "/" + filename, 'rb') as readfile:
				year = str(filename)[3:7]
				o_df = pd.read_csv(readfile, sep=",", header=None)
			yr_df = parse_file(o_df, year)
			comp_df = pd.concat([comp_df, yr_df])
		else:
			continue
		print year, filename

	return comp_df

comp_df = concat_files(mypath, onlyfiles, new_file_ls)

final_df = comp_df
year_df = year_pct_change(comp_df)
total_df = rank_total(comp_df)
year_df = year_pct_change(comp_df)
avg_change_pct_df, diff_df, djia_df = compare_djia(year_df)
year_final_df = stack_reset(year_df, 'yr f')
	
final_df.to_csv(yearly_file_name)
year_final_df.to_csv(year_chg_file_name)
total_df.to_csv(total_file_name)
avg_change_pct_df.to_csv(avg_file_name)
diff_df.to_csv(diff_file_name)
djia_df.to_csv(djia_file_name)

print "\nnew files:",new_file_ls
# the output file will be in the same location as this .py file
print("--- %s seconds ---" % (time.time() - start_time))
print "END"
