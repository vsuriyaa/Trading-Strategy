import pandas as pd
import os

"""
This program implements the "Gross Profit ratio to Market Value" as describe in the instruction that was given to me by AlphaBeta company. 

There are 3 main parts in this program:
1. Calculate the gpr2m value for all the available companies in the ends of each month between 2010-2015. Done by "create_gpr2m_files" function.
2. Create long and short portfolios for each month. Done by "create_portfolio_files" function.
3. Calculate the return of each month's portfolios. Done by "create_return_files" function.
All the other functions are "helper functions".

This program runs asynchronously and each part makes use the files created by the previous part.
"""


def create_gpr2m_files(financial_data, market_data, last_trading_dates, companies):
    """
    Creates for each month the GPR2M value of all the available companies
    :param financial_data: All the financial data we have
    :param market_data: All the market data we have
    :param last_trading_dates:  Last trading dates of each month we have in our market data
    :param companies: All the companies we have in our financial data
    """
    mv_df = market_data[['date', 'companyid', 'price_close_adj', 'shares_out']].copy().loc[
        market_data['date'].isin( last_trading_dates )].reset_index( drop=True )
    mv_df['mv'] = mv_df['price_close_adj'] * mv_df['shares_out']
    for date in last_trading_dates:
        all_companies_gpr2m_data = []
        if date.year != 2010:  # this year is valid for our data
            print( date )  # this helps to get track of the progress of this function
            for company in companies:
                market_value_df = mv_df.loc[(mv_df['date'] == date) & (mv_df['companyid'] == company), 'mv']
                market_value = market_value_df.iloc[0] if len( market_value_df ) > 0 else None
                if market_value is not None:  # This company is available at this date
                    gpr_df = financial_data.loc[
                        (financial_data['companyid'] == company) & (financial_data['filingdate'] <= date)]
                    gpr_lateset_data = gpr_df.loc[
                        gpr_df.groupby( by=['periodenddate'] )['filingdate'].transform( max ) == gpr_df[
                            'filingdate']].sort_values( by=['periodenddate'], ascending=False )
                    clean_gpr_df = gpr_lateset_data.groupby( by=['periodenddate', 'filingdate', 'dataitemname'] )[
                                       'dataitemvalue'].agg( 'mean' ).reset_index().iloc[:8]
                    if len( clean_gpr_df.loc[clean_gpr_df[
                                                 'dataitemname'] == 'Gross Profit'] ) == 4:  # there is enough valid data to calculate gpr2m
                        gross_profit = clean_gpr_df.loc[
                            clean_gpr_df['dataitemname'] == 'Gross Profit', 'dataitemvalue'].sum()
                        total_assets = clean_gpr_df.loc[
                                           clean_gpr_df['dataitemname'] == 'Total Assets', 'dataitemvalue'].sum() / 4
                        gpr = gross_profit / total_assets
                        gpr2m = gpr / market_value
                        company_gpr2m_row = (company, gross_profit, total_assets, market_value, gpr2m)
                        all_companies_gpr2m_data.append( company_gpr2m_row )
            date_gpr2m = pd.DataFrame( all_companies_gpr2m_data,
                                       columns=['company', 'gross profit', 'total assets', 'market value',
                                                'gpr2m'] ).sort_values( by=['gpr2m'], ascending=False )
            save_gpr2m_file( date, date_gpr2m )


def get_trading_dates(market_dates):
    """
    Find the first and last trading dates of all the months in the market data file
    :param market_dates: all the market trading dates
    :return: first_trading_dates: the first trading date of each month
    :return: last_trading_dates: the last trading date of each month
    """
    try:
        all_dates = pd.DatetimeIndex( market_dates.unique() )
        years = all_dates.year.tolist()
        months = all_dates.month.tolist()
        days = all_dates.day.tolist()
        dates = pd.DataFrame( list( zip( years, months, days ) ), columns=['year', 'month', 'day'] ).groupby(
            by=['year', 'month'] )
        first_dates_df = dates['day'].agg( 'min' ).reset_index()
        last_dates_df = dates['day'].agg( 'max' ).reset_index()
        first_trading_dates = pd.to_datetime( first_dates_df ).tolist()
        last_trading_dates = pd.to_datetime( last_dates_df ).tolist()
        return first_trading_dates, last_trading_dates
    except:
        print( "Something went wrong in 'get_trading_dates' function" )


def get_all_companies(data):
    """
    Gets all the companies that we have in the data file
    :param data: The financial data
    :return: List with all the companies id
    """
    try:
        unique_companies = data['companyid'].unique()
        return unique_companies
    except:
        print( "Something went wrong in 'get_all_companies' function" )


def save_gpr2m_file(date, gpr2m_data):
    """
    Saves the files in the desirable location
    :param date: The date of the data we want to save, this is also the name of the directory
    :param gpr2m_data: The data we want to save
    """
    try:
        file_path = output_directory + '\\' + str( date )[:10]
        create_dir( file_path )  # here we save all the relevant files for this date
        file_name = file_path + '\\gpr2m.csv'
        gpr2m_data.to_csv( file_name, index=False )
    except:
        print( "Something went wrong in 'save_gpr2m_file' function" )


def create_portfolio_files(files):
    """
    Creates the long and short portfolios of each month
    :param files: All the data files
    """
    try:
        for file in files:
            date = file[0:10]
            path = output_directory + '\\' + file + '\\gpr2m.csv'
            data_df = pd.read_csv( path )[['company', 'gpr2m']].sort_values( by='gpr2m', ascending=False )
            long_portfolio = data_df.head( 20 )
            short_portfolio = data_df.tail( 20 )
            save_portfolio_files( date, long_portfolio, short_portfolio )
    except:
        print( "Something went wrong in 'create_portfolio_files' function" )


def save_portfolio_files(date, long_portfolio, short_portfolio):
    """
    Saves the portfolio's files
    :param date:  Portfolio's date
    :param long_portfolio: Long portfolio's data
    :param short_portfolio: Short portfolio's data
    :return:
    """
    try:
        file_name = output_directory + '\\' + date + '\\long_portfolio.csv'
        long_portfolio.to_csv( file_name, index=False )
        file_name = output_directory + '\\' + date + '\\short_portfolio.csv'
        short_portfolio.to_csv( file_name, index=False )
    except:
        print( "Something went wrong in 'save_portfolio_files' function" )


def read_gpr2m_files():
    """
    Gets all the gpr2m data files
    :return: The gpr2m data files
    """
    try:
        folder_path = working_directory + '\\' + output_directory
        all_files = [f for f in os.listdir( folder_path )]
        return all_files
    except:
        print( "Something went wrong in 'read_gpr2m_files' function" )


def create_return_files(all_folders):
    """
    Create the portfolio's return data file
    :param all_folders: All the folders with portfolio's data that we want to calculate their return
    """
    try:
        for folder in all_folders:
            if folder != '2014-12-31':  # escape the last trading month in the data
                folder_path = working_directory + '\\' + output_directory + '\\' + folder
                portfolio_files = []
                for f in os.listdir( folder_path ):
                    if 'portfolio' in f:
                        portfolio_files.append( f )
                total_portfolio_return = calculate_total_portfolio_returns( portfolio_files, folder_path )
                my_file = open( folder_path + '\\total_portfolio_return.txt', 'w+' )
                my_file.write( str( total_portfolio_return ) )
    except:
        print( "Something went wrong in 'create_return_files' function" )


def calculate_total_portfolio_returns(portfolio_files, path):
    """
    Calculate the total return of each portfolio
    :param portfolio_files: All the portfolio's files
    :param path: The path in
    :return: The total portfolios return
    """
    try:
        long = [p for p in portfolio_files if 'long' in p]
        short = [p for p in portfolio_files if 'short' in p]
        long_df = pd.read_csv( path + '\\' + long[0] )
        short_df = pd.read_csv( path + '\\' + short[0] )
        date = pd.to_datetime( path[-10:] )  # cast the date of the portfolio to datetime object
        long_return = calculate_portfolio_return_by_type( long_df, date, 'long' )
        short_return = calculate_portfolio_return_by_type( short_df, date, 'short' )
        total_return = long_return + short_return
        return total_return
    except:
        print( "Something went wrong in 'calculate_total_portfolio_returns' function" )


def calculate_portfolio_return_by_type(portfolio, date, type):
    """
    Calculate the portfolio's return based on the portfolio type
    :param portfolio: The portfolio's companies
    :param date: The date relevant to this portfolio, the last trading date of the month
    :param type: The type of the portfolio, can be 'long' or 'short'
    :return: The calculated return of this portfolio
    """
    try:
        portfolio_return = 0
        open_date = get_open_date( date )
        close_date = get_close_date( open_date )
        market_data = market_data_df.loc[(market_data_df['date'] == open_date) | (market_data_df['date'] == close_date)]
        for company in portfolio['company']:
            company_market_data = market_data.loc[(market_data['companyid'] == company)]
            company_open_data = company_market_data.loc[company_market_data['date'] == open_date]
            company_close_data = company_market_data.loc[company_market_data['date'] == close_date]
            if (len( company_open_data ) > 0) & (len( company_close_data ) > 0):
                if type == 'long':  # long position, buy at open and sell at close
                    portfolio_return -= float( company_open_data['price_close_adj'].iloc[0] )
                    portfolio_return += float( company_close_data['price_close_adj'].iloc[0] )
                else:  # short position, sell at open and buy at close
                    portfolio_return += float( company_open_data['price_close_adj'].iloc[0] )
                    portfolio_return -= float( company_close_data['price_close_adj'].iloc[0] )
        return portfolio_return
    except:
        print( "Something went wrong in 'calculate_portfolio_return_by_type' function" )


def get_open_date(date):
    """
    Find the first trading date of the closest month
    :param date: The portfolio's date
    :return: The relevant open date
    """
    try:
        index = all_last_trading_dates.index( date )
        if (index + 1) < len( all_first_trading_dates ):  # we want to skip the last month in the data
            next_date = all_first_trading_dates[index + 1]
            return next_date
        return
    except:
        print( "Something went wrong in 'get_open_date' function" )


def get_close_date(open_date):
    """
    Find the first trading date of the closest month
    :param open_date: The portfolio's open date
    :return: The relevant close date
    """
    try:
        index = all_first_trading_dates.index( open_date )
        if (index + 1) < len( all_first_trading_dates ):  # we want to skip the last month in the data
            next_date = all_first_trading_dates[index + 1]
            return next_date
        return
    except:
        print( "Something went wrong in 'get_close_date' function" )


def create_dir(name):
    """
    Creates new directory if it not exists
    :param name: The directory's name
    """
    try:
        if not os.path.exists( name ):
            os.mkdir( name )
    except:
        print( "Something went wrong in 'create_dir' function" )


if __name__ == '__main__':
    working_directory = os.getcwd()
    output_directory = 'output'  # here we save all the program outputs
    create_dir( output_directory )

    financial_data_df = pd.read_csv( 'data\\financial_data.csv' ).drop(
        columns=['restatementtypename'] ).drop_duplicates().reset_index( drop=True )
    market_data_df = pd.read_csv( 'data\\market_data.csv' ).drop_duplicates().reset_index( drop=True )

    market_data_df['date'] = pd.to_datetime( market_data_df['date'] )
    financial_data_df['filingdate'] = pd.to_datetime( financial_data_df['filingdate'] )
    all_first_trading_dates, all_last_trading_dates = get_trading_dates( market_data_df['date'] )
    all_companies = get_all_companies( financial_data_df )

    """
        Uncomment this function if you want to recreate the gpr2m files.
        Note: This part can take up to 3 minutes, depends on your computer performance. 
    """
    create_gpr2m_files( financial_data_df, market_data_df, all_last_trading_dates,
                        all_companies )  # create gpr2m files for all the companies in all the dates
    data_files = read_gpr2m_files()  # get the gpr2m data files
    create_portfolio_files( data_files )  # create long and short portfolios
    create_return_files( data_files )  # create total return files
