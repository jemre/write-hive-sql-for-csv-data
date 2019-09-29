from glob import glob


def get_table_name(csv): return csv.split('.')[0]


def get_headers(csv_file): return csv_file.readline().strip().split(',')


def get_sql_filename(csv): return 'create_' + get_table_name(csv) + '_table_load_data.sql'


def add_drop_table(sql_file, table_name): sql_file.write('DROP TABLE {}; \n\n'.format(table_name))


def add_create_table(sql_file, table_name): sql_file.write('CREATE TABLE IF NOT EXISTS {}; \n'.format(table_name))


def parse_csv():
    print('Getting all of the CSV files')

    csv_list = glob('*.csv')
    sql_filename_list = []

    for csv in csv_list:
        csv_file = open(csv)

        sql_filename = get_sql_filename(csv)
        sql_filename_list.append(sql_filename)
        sql_file = open(sql_filename, 'w')

        # Headers are the first row of the csv
        headers = get_headers(csv_file)

        # Table name should be the name of the file without the .csv on the end
        table_name = get_table_name(csv)

        add_drop_table(sql_file, table_name)
        add_create_table(sql_file, table_name)

        csv_file.close()
        sql_file.close()

    for filename in sql_filename_list:
        pass

if __name__ == '__main__':
    parse_csv()
