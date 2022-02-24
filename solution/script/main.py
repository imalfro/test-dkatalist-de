import os, json
import pandas as pd
import glob
from datetime import datetime

pd.set_option('display.max_columns', None)

def clean(temp_df):
    df = pd.DataFrame()
    columns = [col.replace('set.','').replace('data.','') for col in temp_df.columns]
    
    for col in set(columns):
        if ('data.'+col in temp_df.columns) and ('set.'+col in temp_df.columns) :
            df[col] = temp_df['data.'+col].combine_first(temp_df['set.'+col])
        
        if ('data.'+col in temp_df.columns) and ('set.'+col not in temp_df.columns) :
            df[col] = temp_df['data.'+col]
        
        if ('data.'+col not in temp_df.columns) and ('set.'+col in temp_df.columns) :
            df[col] = temp_df['set.'+col]
            
        if ('data.'+col not in temp_df.columns) and ('set.'+col not in temp_df.columns) :
            df[col] = temp_df[col]
            
    return df

def get_df(path_to_json):
    temp = pd.DataFrame()
    dfs = []

    json_pattern = os.path.join(path_to_json,'*.json')
    file_list = glob.glob(json_pattern)
    for file_str in file_list:
        with open(file_str, 'r', encoding="utf-8") as json_file:
            json_work = json.load(json_file)
            data = pd.json_normalize(json_work)
            dfs.append(data)
            
    temp = pd.concat(dfs, ignore_index=True)
    df = clean(temp)    

    return df

if __name__ == '__main__':
    #get dataframe for accounts
    accounts_path = '../../data/accounts/' 
    df_accounts = get_df(accounts_path)
    df_accounts = df_accounts.set_index(['id','ts'])
    df_accounts = df_accounts.sort_index()
    df_accounts = df_accounts.ffill(axis = 0)

    #Print historical data ACCOUNTS 
    print('--- HISTORICAL ACCOUNTS data ---')
    print(df_accounts)
    print('')
    
    #get dataframe for cards
    cards_path = '../../data/cards/' 
    df_cards = get_df(cards_path)
    df_cards = df_cards.set_index(['id','ts'])
    df_cards = df_cards.sort_index()
    #to fill the gap on all fields
    #because update operations only contains updated column
    #this is to assume,
    #when the update operation conducted, the changes only applied to the specified column
    #all else are still the same like before
    #EXCEPT FOR CREDIT_USED, because it was the occuring transaction at that time
    df_cards[['credit_used']] = df_cards[['credit_used']].fillna(0)
    df_cards = df_cards.ffill(axis = 0)

    #Print historical data CARDS 
    print('--- HISTORICAL CARDS data ---')
    print('')
    print(df_cards)
    print('')

    #get dataframe for savings
    savings_path = '../../data/savings_accounts/' 
    df_savings = get_df(savings_path)
    df_savings = df_savings.set_index(['id','ts'])
    df_savings = df_savings.sort_index()
    #to fill the gap on all fields
    #because update operations only contains updated column
    #this is to assume,
    #when the update operation conducted, the changes only applied to the specified column
    #all else are still the same like before
    df_savings = df_savings.ffill(axis = 0) 
    
    #Print HISTOICAL data SAVINGS
    print('--- HISTORICAL SAVINGS data ---')
    print('')
    print(df_savings)
    print('')

    #JOIN ALL THREE TABLES
    print('--- JOINING ALL TABLES FOR THE LATEST DATA ---')
    print('')
    #to avoid overlapping columns, EXCEPT FOR KEY COLUMN
    card_columns = df_cards.columns.difference(df_accounts.columns[~df_accounts.columns.isin(['card_id','op'])])
    sa_columns = df_savings.columns.difference(df_accounts.columns[~df_accounts.columns.isin(['savings_account_id','op'])])


    #joining all table
    #using full join (outer) because some timestamp might not overlap with each other
    df_account_cards = pd.merge(df_accounts,df_cards[card_columns], how='outer', on=['card_id','ts'], suffixes=('__ACC', '__CARD'))
    df_all = pd.merge(df_account_cards,df_savings[sa_columns], how='outer', on=['savings_account_id','ts'], suffixes=('__CARD', '__SA'))
    #when the timestamp are not overlap within each table, this may cause gap while joining
    #assuming all other columns still has the same values to the latest value of the respective column
    #EXCEPT FOR CREDIT_USED, because it was the occuring transaction at that point of time
    df_all[['credit_used']] = df_all[['credit_used']].fillna(0)
    df_all = df_all.ffill(axis = 0)

    print(df_all)
    
    print('')
    print('--- TO CONCLUDE ---')
    print('')
    print('This account have 1 time changing phone number, email, and address')
    print('')
    print('CARD EVENTS AND TRANSACTIONS:')
    print('')
    print('This account have 2 cards that was PENDING at THE SAME TIME')
    print('')
    print('The card C1 activeted FIRST, WITH MONTHLY LIMIT 3000') 
    print('The card C1 was active until and closed at ts:1579078800000')
    print('While the card C1 was still active, C2 was not active')
    print('the 1st transaction usage of card C1: 12000 of monthly limit 30000 at ts:1578313800000')
    print('the remaining CARD BALANCE should be 30000-12000 = 18000')
    print('the 2nd transaction usage of card C1: 19000 of monthly limit 30000 at ts:1578420000000')
    print('the remaining CARD BALANCE should be 18000-19000 = (-1000)')
    print('')
    print('when the card C1 was closed, card C2 activated WITH MONTHLY LIMIT 70000')
    print('card C2 activated at ts:1579298400000')
    print('assuming transaction is carri-over from previous card then:')
    print('the remaining CARD BALANCE should be 70000-30000-19000 = 39000')
    print('the 1st transaction usage of card C2: 37000 of monthly limit 70000 at ts:1579361400000')
    print('the remaining CARD BALANCE should be 39000-37000 = 2000')
    print('')
    print('SAVING EVENTS AND TRANSACTIONS:')
    print('')
    print('Saving account is created at ts:1577890800000')
    print('')
    print('At ts:1577955600000 the balance is set 15000') 
    print('-- it becomes HIGHER than previous balance which was NULL')
    print('-- there was a DEPOSIT/FUND-RECEIVING TRANSACTION in amount 15000')
    print('at ts:1578648600000 the balance is set 40000')
    print('-- it becomes HIGHER than previous balance which was 15000')
    print('-- there was a DEPOSIT/FUND-RECEIVING TRANSACTION in amount 40000-15000 = 35000')
    print('at ts:1578654000000 the balance is set 21000') 
    print('-- it becomes LOWER than previous balance which was 40000')
    print('-- there was a WITHDRAWAL/FUND-TRANSFER TRANSACTION in amount 40000-21000 = 19000')
    print('at ts:1579505400000 the balance is set 33000')
    print('-- it becomes HIGHER than previous balance which was 21000')
    print('-- there was a DEPOSIT/FUND-RECEIVING TRANSACTION in amount 33000-21000 = 12000')
    print('')
