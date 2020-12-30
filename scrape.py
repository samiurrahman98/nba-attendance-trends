import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from pathlib import Path

for year in range(1981, 2019):
    url = 'https://www.basketball-reference.com/leagues/NBA_{}_games.html'.format(year)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    month_filter_container = soup.find('div', class_='filter')

    month_range = []
    for div in month_filter_container.find_all('div'):
        month_range.append(re.sub('[^a-z]+', '', div.get_text().lower()))

    print(month_range)

    for month in month_range:
        url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year, month)
        page = requests.get(url)

        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find(id='schedule')
        body = table.find('tbody')

        df = pd.DataFrame(columns=['Date', 'Visitor/Neutral', 'PTS_1', 'Home/Neutral', 'PTS_2', '_1', '_2', 'Attend.', 'Notes'])

        for index, row in enumerate(body.find_all('tr')):
            row_data = []
            for item in row:
                row_data.append(item.get_text())
            if len(row_data) == 9:
                df_row = {
                    'Date': row_data[0],
                    'Visitor/Neutral': row_data[1],
                    'PTS_1': row_data[2],
                    'Home/Neutral': row_data[3],
                    'PTS_2': row_data[4],
                    '_1': row_data[5],
                    '_2': row_data[6],
                    'Attend.': row_data[7],
                    'Notes': row_data[8],
                }
                df.loc[index] = df_row

        output_file = '{}.csv'.format(month)
        output_directory = Path('/Users/samiurrahman98/Desktop/Programming/nba-attendance-trends/output/{}/'.format(year))
        output_directory.mkdir(parents=True, exist_ok=True)
        df.to_csv(path_or_buf=(output_directory / output_file), index=False)