from ..utils import get_wiki_table_df, wiki_components_list_to_df


DJI_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
DJT_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Transportation_Average'
DJU_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Utility_Average'


def get_dji_df():
    """A DataFrame of DJI components

    Indexed by ticker with columns: company_name, exchange, industry, date_added, notes
    """
    df = get_wiki_table_df(DJI_URL).rename(columns={'company': 'company_name',
                                                    'symbol': 'ticker'})
    df.ticker = [x.split(':')[-1].strip() for x in df.ticker]
    return df.set_index('ticker')


def get_djt_df():
    """A DataFrame of DJT components

    Indexed by ticker with columns: company_name, industry
    """
    df = get_wiki_table_df(DJT_URL, index_col='ticker')
    return df.rename(columns={'corporation': 'company_name'})


def get_dju_df():
    """A DataFrame of DJU components

    Indexed by ticker with columns: company_name
    """
    df = get_wiki_table_df(DJU_URL).rename(columns={'company': 'company_name'})
    return df.set_index('ticker')

    # old working code (for <ul> components instead of <table>)
    # soup = get_soup(DJU_URL)
    # components_header = soup.find('span', attrs={'id': 'Components'}).parent
    # components_list = components_header.find_next_sibling('ul')
    # return wiki_components_list_to_df(components_list)
