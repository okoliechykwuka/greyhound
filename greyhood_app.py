# Import modules
import pandas as pd
import re
import os
import warnings
import numpy as np
from datetime import date
import base64
import requests
from urllib.error import URLError
from lxml import html
from requests.exceptions import ConnectionError
warnings.filterwarnings("ignore")
pd.set_option('display.float_format', lambda x: '%.2f' % x)

pd.options.display.max_rows = None
pd.options.display.max_columns = 100
import streamlit as st

# set project title
st.title('Automated Greyhound Data Scraper')

# definne all utility functions for betfair and greyhound
@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) #suppress_st_warning=True
def extract_betfair_data(betfair, data_date):
    
    try:
        date_str = str(data_date)
        today = date.today()
        if str(today) >= date_str:
            day = ''.join(date_str.split('-')[::-1])
            betfair_url = '{}{}{}'.format(betfair,day,'.csv')
            df = pd.read_csv(betfair_url)
            df.reset_index(drop=True, inplace=True)
            df['DATE'] = pd.to_datetime(df['EVENT_DT']).dt.date 
            df['DATE'] = df['DATE'].apply(lambda x: x.strftime('%d/%m/%Y'))
            df['Time'] = df['EVENT_DT'].str.split(' ').str[1] 
            df['Year'] = pd.to_datetime(df['EVENT_DT']).dt.year
            df['Month'] = pd.to_datetime(df['EVENT_DT']).dt.month
            dmap = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            df['Month'] = df['Month'].map(dmap) 
            

            return df
        else:
            st.write("Hey! the date you provided exceeded current date")      
    except URLError as e:
        st.write('You need a Connection to the internet to scrape betfair data') 


#Process BetFair file and extract the appropraite columns
@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True)
def process_betfair_data(df):

    """function takes in a dataframe, process and extract the needed column.
       return: returns a clean processed dataframe with newly created columns.
    """
    try:
        betfair_df = pd.DataFrame()
        # joining new column in dataframe # .startswith function used to check
        df['MENU_HINT_START_WITH_AUS'] = pd.DataFrame(map(lambda x: x.startswith('AUS'), df['MENU_HINT']))
        df = df[df['MENU_HINT_START_WITH_AUS'] == True]
        df.drop(df.iloc[:,8:17].columns,axis=1,inplace=True)
        df['SELECTION_TRAP'] = pd.to_numeric(df['SELECTION_NAME'].str.split('.').str[0])
        df['Track'] = df['MENU_HINT'].str.split(' ').str[2].str.strip()
        df['#'] = df['EVENT_NAME'].str.split(' ').str[0].str.strip()
        df['Distance'] = df['EVENT_NAME'].str.split(' ').str[1].str.strip()
        df['Betfair Grade'] = df['EVENT_NAME'].str.split(' ').str[2].str.strip()
        
        betfair_df['EVENT_ID'] = df['EVENT_ID'].unique()
        betfair_df[['Date','Time','Year','Month','Track','#','Distance','Betfair Grade']] = df.sort_index(ascending=True).groupby('EVENT_ID',sort=False) \
                            [['DATE','Time','Year','Month','Track','#','Distance','Betfair Grade']].last().reset_index()\
                            [['DATE','Time','Year','Month','Track','#','Distance','Betfair Grade']]
        betfair_df['Runners'] = list(df.groupby('EVENT_ID',sort=False)['SELECTION_ID'].nunique())
        filter_df = df[df['WIN_LOSE']==1].sort_values(by='WIN_LOSE', ascending=False).reset_index()[['EVENT_ID','SELECTION_TRAP','BSP']]
        betfair_df = betfair_df.merge(filter_df, on = 'EVENT_ID', how = 'left')
        betfair_df.rename(columns={'SELECTION_TRAP': 'Win Trap','BSP':'Win BSP'},inplace=True, errors='raise')
        betfair_df['Win BSP'] = round(betfair_df['Win BSP'],2)
        
        pivot_df = round(df.pivot_table(index="EVENT_ID",columns="SELECTION_TRAP",values="BSP",fill_value=0).reset_index(),2)
        betfair_df = betfair_df.merge(pivot_df, on = 'EVENT_ID', how = 'left')
        # labels to assign odds bands 
        labels = ['0', '1.0 - 1.19', '1.2 – 1.39', '1.4 – 1.59', '1.6 – 1.79', 
                '1.8 – 1.99', '2.0 – 2.99', '3.0 – 3.99', '4.0 – 4.99', '5.0 – 5.99', 
                '6.0 – 6.99', '7.0 – 7.99', '8.0 – 8.99', '9.0 – 9.99', '10.0+'
                ]
        # Define the edges between bins
        bins = [0, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 3.0, 4.0,5.0, 6.0, 7.0, 8.0, 9.0, 10.0, np.inf]
        # pd.cut each column, with each bin closed on left and open on right
        res = betfair_df.filter(regex="\d+", axis=1).apply(lambda x: pd.cut(x, bins=bins, labels=labels, right=False).astype('object'),).replace('0',0.00)

        # rename columns and print result
        res.columns = [f'Trap{i+1} Odds Band' for i in range(betfair_df.filter(regex="\d+", axis=1).shape[1])]
        res.reset_index(drop=True, inplace=True)
    #     res[res.columns] = res[res.columns].astype('object') 
        betfair_df = pd.concat([betfair_df,res],axis=1)
        betfair_df['Blank_2'] = ''
        
        cols = list(betfair_df.filter(regex="^\d+$", axis=1).columns)
        
        betfair_df[[f'Fav{i}' for i in range(1, len(cols) + 1)]] = round(pd.DataFrame(np.sort(betfair_df[cols].where(betfair_df[cols] > 0), axis=1)).fillna(0),2)
        
        res1 = betfair_df.filter(like="Fav", axis=1).apply(lambda x: pd.cut(x, bins=bins, labels=labels, right=False).astype('object')).replace('0',0.00)
        res1.columns = [f'Fav{i+1} Odds Band' for i in range(betfair_df.filter(like="Fav", axis=1).shape[1])]
        res1.reset_index(drop=True, inplace=True)
        betfair_df = pd.concat([betfair_df,res1],axis=1)
        betfair_df['Blank_3'] = ''
    
        return betfair_df

    except (TypeError,ValueError,KeyError):
        st.write("No date selected yet")
    

# Greyhound functions
@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
def get_page(url):
    """fxn take page url and return the links to the acticle(Field) we 
       want to scrape in a list.
    """
    page = requests.get(url)
    tree = html.fromstring(page.content)
    my_list = tree.xpath('//tbody/tr/td[2]/a/@href') # grab all link
    # print('Length of all links = ', len(my_list))
    my_url = [page.url.split('/form-guides')[0] + str(s) for s in my_list]
    return my_url

@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
def extract_data(my_url):
    """
    fxn take a list of urls and extract the needed infomation from 
    greyhound website. 
    return: a list with the extracted field
    """
    new_list = []
    try:
        for t in my_url:
            s = requests.Session()
            page_detail = s.get(t,stream=True)
            page_detail = requests.get(t)
            tree_1 = html.fromstring(page_detail.content)
            title = ''.join(tree_1.xpath('//div/h1[@class="title"]/text()'))
            race_number = tree_1.xpath("//tr[@id = 'tableHeader']/td[1]/text()")
            Distance = tree_1.xpath("//tr[@id = 'tableHeader']/td[3]/text()")
            TGR_Grade = tree_1.xpath("//tr[@id = 'tableHeader']/td[4]/text()")
            TGR1 = tree_1.xpath("//tbody/tr[@class='fieldsTableRow raceTipsRow']//div/span[1]/text()")
            TGR2 = tree_1.xpath("//tbody/tr[@class='fieldsTableRow raceTipsRow']//div/span[2]/text()")
            TGR3 = tree_1.xpath("//tbody/tr[@class='fieldsTableRow raceTipsRow']//div/span[3]/text()")
            TGR4 = tree_1.xpath("//tbody/tr[@class='fieldsTableRow raceTipsRow']//div/span[4]/text()")

            # clean_title = title.split(' ')[0].strip()
            #clean title and extract track number
            Track = title.split(' ')[0].strip()
            #clean title and extract track date
            date = title.split('-')[1].strip()
            #clean title and extract track year
            year = pd.to_datetime('now').year 
            #convert date to pandas datetime
            race_date =  pd.to_datetime(date + ' ' + str(year)).strftime('%d/%m/%Y')
            #extract race number
            new_rn = []
            for number in race_number:
                match = re.search(r'^(.).*?(\d+)$', number)
                new_rn.append(match.group(1) + match.group(2))
            new_list.append((race_date,Track,new_rn,Distance,TGR_Grade,TGR1,TGR2,TGR3,TGR4))
        
            
        return new_list
            
    except ConnectionError as e:
        st.write('Connection error, connect to a stronger network or reload the page')

@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
def convert_to_dict(my_list):
    try:
        new_dict = {
        'Date': [],
        'Track': [],
        '#': [],
        'Distance': [],
        'TGR Grade': [],
        'TGR1': [],
        'TGR2': [],
        'TGR3': [],
        'TGR4': [],
        }

        for date, track, race, distance, grade,tgr1,tgr2,tgr3,tgr4  in my_list:
            new_dict['Date'].append(date)
            new_dict['Track'].append(track)
            new_dict['#'].append(race)
            new_dict['Distance'].append(distance)
            new_dict['TGR Grade'].append(grade)
            new_dict['TGR1'].append(tgr1)
            new_dict['TGR2'].append(tgr2)
            new_dict['TGR3'].append(tgr3)
            new_dict['TGR4'].append(tgr4)
        
        return new_dict
    except TypeError:
        'Greywood data was not scraped poor internet connection'
        
    

@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True)         
def read_greyhound_recorder_csv(data):
    try:
        if 'TGR1'  in data:
            df = pd.DataFrame(data)
            df[['#','Distance','TGR Grade','TGR1','TGR2','TGR3','TGR4']] = df[['#','Distance','TGR Grade','TGR1','TGR2','TGR3','TGR4']].applymap(\
                                                                            lambda x: ','.join(map(str,x)))
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df.dropna(axis = 0, how ='any',inplace=True)
            df.drop(['Date', 'Track'], axis = 1, inplace = True) 
            df.reset_index(inplace = True)
            df['#'] = df['#'].str.split(',')
            df['Distance'] = df['Distance'].str.split(',')
            df['TGR Grade'] = df['TGR Grade'].str.split(',')
            df['TGR1'] = df['TGR1'].str.split(',')
            df['TGR2'] = df['TGR2'].str.split(',')
            df['TGR3'] = df['TGR3'].str.split(',')
            df['TGR4'] = df['TGR4'].str.split(',')
            #explode columns using the row values in columns aside from (Date and Track)
            df = (df.set_index(['index']).apply(pd.Series.explode).reset_index())
            #set columns values to numeric
            df[['TGR1','TGR2','TGR3','TGR4']] = df[['TGR1','TGR2','TGR3','TGR4']].apply(pd.to_numeric, errors='coerce')
            greyhound_df = df[['#','TGR Grade','TGR1','TGR2','TGR3','TGR4']]    
        else:
            st.write('TGR1,TGR2,TGR3 and TGR4  not in csv column')

        return greyhound_df

    except (TypeError,ValueError,KeyError):
        st.write('Greyhood data was not scraped due to poor internet connection')

# Consolidate betfair race and greyhound data
@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
def consolidate_betfair_race_data(betfair_scrape,greyhound_df):
    st.write(betfair_scrape)
  
    # if betfair_scrape == None:
    #     return st.write('No data was scraped for Greyhound')
        
    # st.write(repr(betfair_scrape))
    try:
        output_df = betfair_scrape.merge(greyhound_df, on = '#', how = 'left').replace(np.nan,0.00)
        output_df = output_df.drop_duplicates(subset = 'EVENT_ID', keep = 'first')
        output_df.reset_index(drop=True, inplace=True)
    #     output_df["Date"]= pd.to_datetime(output_df["Date"])
        
    #     # extract columns 1,2,3..10 into a numpy array with a zeros column stacked on the left
        vals_tgr = np.column_stack((np.zeros(len(output_df)), output_df[list(output_df.filter(regex="^\d+$", axis=1).columns)]))
    #     # use TGR1 values as the column index to extract corresponding values
        output_df['TGR1 BSP'] = np.round(vals_tgr[np.arange(len(output_df)), output_df.TGR1.values.astype(int)],2)
        output_df['TGR2 BSP'] = np.round(vals_tgr[np.arange(len(output_df)), output_df.TGR2.values.astype(int)],2)
        output_df['TGR3 BSP'] = np.round(vals_tgr[np.arange(len(output_df)), output_df.TGR3.values.astype(int)],2)
        output_df['TGR4 BSP'] = np.round(vals_tgr[np.arange(len(output_df)), output_df.TGR4.values.astype(int)],2)
        # labels to assign odds bands 
        labels = ['0', '1.0 - 1.19', '1.2 – 1.39', '1.4 – 1.59', '1.6 – 1.79', 
                '1.8 – 1.99', '2.0 – 2.99', '3.0 – 3.99', '4.0 – 4.99', '5.0 – 5.99', 
                '6.0 – 6.99', '7.0 – 7.99', '8.0 – 8.99', '9.0 – 9.99', '10.0+']
        # Define the edges between bins
        bins = [0, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 3.0, 4.0,5.0, 6.0, 7.0, 8.0, 9.0, 10.0, np.inf]
        # pd.cut each column, with each bin closed on left and open on right
        res = output_df.filter(regex="TGR\d+ B", axis=1).apply(lambda x: pd.cut(x, bins=bins, labels=labels, right=False))

        # rename columns and print result
        res.columns = [f'TGR{i+1} Odds Band' for i in range(output_df.filter(regex="TGR\d+ B", axis=1).shape[1])]
        res.reset_index(drop=True, inplace=True)
        output_df = pd.concat([output_df,res],axis=1)
        
        empty_df = pd.DataFrame(columns=['A','B','C'])
        final_output_df = pd.concat([empty_df,output_df])
        final_output_df.insert(14, 'Blank_1', '')

        return final_output_df

    except (AttributeError,TypeError):#IndexError,AttributeError,TypeError,ValueError
        st.write('No data have been scraped yet. scrape betfair and greyhound by pressing the buttons above')
#append consolidated file to a csv
# @st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
# def appendDFToCSV_void(df, csvFilePath, sep=",", encoding='cp1252'):
#     try:
#         if not os.path.isfile(csvFilePath):
#             df.to_csv(csvFilePath, mode='a', index=False, sep=sep,encoding=encoding)
#         elif len(df.columns) != len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns):
#             raise Exception("Columns do not match!! Dataframe has " + str(len(df.columns)) + " columns. CSV file has " /
#                                     + str(len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + " columns.")
#         elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
#             raise Exception("Columns and column order of dataframe and csv file do not match!!")
#         else:
#             df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False, encoding= encoding)
#     except PermissionError:
#         st.write('You have to close the consolidated excel file before appending to it.')
@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True) 
def get_table_download_link(df,data = "data.csv"):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    try:
        csv = df.to_csv(mode='w', index=False, sep=",", encoding="utf-8-sig",float_format="%.2f")
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}" download="{data}">Download csv file</a>'
        return href
    except (AttributeError, TypeError,ValueError,KeyError):
        st.write('You entered the wrong date')
    
def main():


    date_input = st.date_input('Enter Betfair data Date: ')
    betfair_url ="https://promo.betfair.com/betfairsp/prices/dwbfgreyhoundwin"
    df = extract_betfair_data(betfair_url, date_input)
    betfair_df = process_betfair_data(df)
    save = get_table_download_link(df)
    if st.button('Summit'):
        st.markdown(f"You selected the following date: {date_input}")
        
    if st.checkbox('Show BetFair processed file'):  
        st.write(betfair_df)

    if st.checkbox("Do you like to download betfair data to your pc ?."):
        if st.button("Yes"):
            st.markdown(get_table_download_link(df,'Betfair.csv'), unsafe_allow_html=True)
        if st.button("No"):
            pass
    

        # Scrape Greyhound data
    greyhound_url = 'http://thegreyhoundrecorder.com.au/form-guides/'
    my_url = get_page(greyhound_url)
    table_information = extract_data(my_url)
    if st.button('Scrape Greyhound data'):
        data_load_state = st.text('Loading data...done')

    
    greyhound_dict = convert_to_dict(table_information)
    greyhound_df = read_greyhound_recorder_csv(greyhound_dict)

    if st.checkbox('Show Greyhound processed file'):
        st.write(greyhound_df)


    if st.checkbox("Do you like to download Greyhood data to your pc?."):
        if st.button("Yeah"):
            st.markdown(get_table_download_link(greyhound_df,'Greyhound.csv'), unsafe_allow_html=True)
        if st.button("Nope"):
            pass

    # consolidating betfair race and greyhound data
    final_output_df = consolidate_betfair_race_data(betfair_df,greyhound_df)
    if st.checkbox('Preview Consolidated file'): 
        try:
            st.write(final_output_df.astype('object'))
        except AttributeError:
            print('No data was scraped for betfair or the date you entered exceeded the current date')
        
    #write file to  a csv
    st.subheader('save consolidated file')
    if st.button("Save"):
        st.markdown(get_table_download_link(final_output_df,'Consolidated_output.csv'), unsafe_allow_html=True)

# About
st.sidebar.subheader("About App")
st.sidebar.text("This app helps to automate the extraction of data from two various sourrces, Betfair and Greyhound. The data is further processed and consolidated into a final table")


# st.sidebar.text("Portfolio link : https://chukypedro.netlify.com")

if __name__ == '__main__':
    main()




