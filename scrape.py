import requests
import csv
import pandas as pd
import re
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

for year in range(1981, 2021):
    url = 'https://www.basketball-reference.com/leagues/NBA_{}_games.html'.format(year)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    month_filter_container = soup.find('div', class_='filter')

    month_range = []
    for div in month_filter_container.find_all('div'):
        month_range.append(re.sub('[^a-z]+', '', div.get_text().lower()))

    for month in month_range:
        url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year, month)
        page = requests.get(url)

        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find(id='schedule')
        body = table.find('tbody')

        df = pd.DataFrame(columns=['Date', 'Start (ET)', 'Visitor/Neutral', 'Visitor/Neutral PTS', 'Home/Neutral', 'Home/Neutral PTS', 'Attendance', 'Notes'])

        for index, row in enumerate(body.find_all('tr')):
            row_data = []
            for item in row:
                row_data.append(item.get_text())

            if (len(row_data) < 9):
                continue

            include_start_time = True if len(row_data) == 10 else False

            df.loc[index, 'Date'] = datetime.strptime(row_data[0], '%a, %b %d, %Y').strftime('%Y-%m-%d')
            df.loc[index, 'Start (ET)'] = datetime.strptime(row_data[1] + 'm', '%I:%M%p').strftime('%H:%M') if (row_data[1] and include_start_time) else None
        
            subtract_index = 1 if not include_start_time else 0

            df.loc[index, 'Visitor/Neutral'] = row_data[2-subtract_index]
            df.loc[index, 'Visitor/Neutral PTS'] = int(row_data[3-subtract_index])
            df.loc[index, 'Home/Neutral'] = row_data[4-subtract_index]
            df.loc[index, 'Home/Neutral PTS'] = int(row_data[5-subtract_index])
            df.loc[index, 'Attendance'] = int(row_data[8-subtract_index].replace(',', '')) if row_data[8-subtract_index] else None
            df.loc[index, 'Notes'] = row_data[9-subtract_index]

        output_file = '{}.csv'.format(month)
        output_directory = Path('/Users/samiurrahman98/Desktop/Programming/nba-attendance-trends/output/{}/'.format(year))
        output_directory.mkdir(parents=True, exist_ok=True)
        df.to_csv(path_or_buf=(output_directory / output_file), index=False, quoting=csv.QUOTE_NONNUMERIC)