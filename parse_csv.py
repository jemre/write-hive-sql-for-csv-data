from glob import glob
import os

def get_table_name(csv): return csv.split('.')[0]

def get_next_row(csv_file): return csv_file.readline().strip().split(',')

def get_sql_filename(csv): return 'create_' + get_table_name(csv) + '_table_load_data.sql'

def add_drop_table(sql_file, table_name): sql_file.write('DROP TABLE {};\n'.format(table_name))

def add_create_table(sql_file, table_name): sql_file.write('CREATE TABLE IF NOT EXISTS {}\n'.format(table_name))

def is_integer(x):
    try:
        a = float(x)
        b = int(x)
    except ValueError:
        return False
    else:
        return a == b

def is_floating(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True

def get_header_type(first_data_element):

    if is_integer(first_data_element):
        return 'int'
    elif is_floating(first_data_element):
        return 'double'
    else:
        return 'string'

def get_line_ending(index, header_length):
    if index + 1 == header_length:
        return ''
    else:
        return ','

def add_headers(sql_file, headers, first_data_row):
    header_length = len(headers)
    for index in range(header_length):
        sql_file.write('`{}`'.format(headers[index]) + ' ' + get_header_type(first_data_row[index]) + get_line_ending(index, header_length) + '\n')

def add_footer(sql_file):
    sql_file.write('ROW FORMAT DELIMITED\n')
    sql_file.write("FIELDS TERMINATED BY ','\n")
    sql_file.write("LINES TERMINATED BY '\\n'\n")
    sql_file.write('STORED AS TEXTFILE\n')
    sql_file.write('tblproperties ("skip.header.line.count"="1");\n')
    # sql_file.write('\n')

def add_load_data(sql_file, table_name):
    sql_file.write("Load data local inpath '{}.csv' into table {};".format(table_name, table_name))
    # sql_file.write('\n\n')

def parse_csv():
    print('Getting all of the CSV files...')

    csv_list = glob('*.csv')
    sql_filename_list = []

    for csv in csv_list:
        csv_file = open(csv)

        sql_filename = get_sql_filename(csv)
        sql_filename_list.append(sql_filename)
        sql_file = open(sql_filename, 'w')

        # Headers are the first row of the csv
        headers = get_next_row(csv_file)

        # Get the first row data (under header row).  We'll use this to determine the types.
        first_data_row = get_next_row(csv_file)

        # Table name should be the name of the file without the .csv on the end
        table_name = get_table_name(csv)

        add_drop_table(sql_file, table_name)
        add_create_table(sql_file, table_name)
        sql_file.write('(\n')
        add_headers(sql_file, headers, first_data_row)
        sql_file.write(')\n')
        add_footer(sql_file)
        add_load_data(sql_file, table_name)

        csv_file.close()
        sql_file.close()

        print('parsed {}!'.format(csv))

    # Run all the sql statements...
    for filename in sql_filename_list:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('running sql for: {}'.format(filename))
        os.system('hive -f {}'.format(filename))

def run_query_one():
    print('Running Query One.....')

    sql = '''
    SELECT county_name, count(fatalities) as count
    FROM crash_location
    GROUP BY county_Name
    HAVING county_name != 'NULL'
    AND TRIM(county_name) != ''
    ORDER BY count desc
    LIMIT 10;
    '''

    sql_file = open('query_one.sql', 'w')
    sql_file.write(sql)
    sql_file.close()

    os.system('hive -f query_one.sql > query_one_results.csv')


if __name__ == '__main__':
    parse_csv()
    run_query_one()
