import mysql.connector
from mysql.connector import errorcode
import sys
import time


def get_configuration(config_filename):
    try:
        config_file = open(config_filename, "r")
    except FileNotFoundError:
        print("File", config_filename, "does not exist.")
        sys.exit()

    connection_variables = {}
    for line in config_file:
        line = line.strip().split("=")
        connection_variables[line[0]] = line[1]

    # Convert port value to integer.
    try:
        connection_variables["port"] = int(connection_variables["port"])
    except ValueError:
        print("Port number must be an integer, eg. 3306\nCheck %s file." % config_filename)
        sys.exit()

    config_file.close()
    return connection_variables


def open_connection(configuration_dictionary):
    try:
        connection = mysql.connector.connect(**configuration_dictionary)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print(err.msg)
            print("Something is wrong with your user name or password.\nCheck configuration file.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(err.msg)
            print("Check configuration file.")
        elif err.errno == errorcode.CR_CONN_HOST_ERROR:
            print(err.msg)
            print("Check IP address and port in configuration file.")
        else:
            print(err)
        sys.exit()

    return connection


def write_many_db(cursor, query, seq_of_params):
    try:
        cursor.executemany(query, seq_of_params)
    except mysql.connector.Error as err:
        print(err.msg)
        # make sure to close connections if error occurs while putting data to database
        database_source.close()
        database_target.close()
        print("Connections closed.")
        sys.exit()


if __name__ == '__main__':
    start_time = time.time()

    # make sure user provided correct number of arguments
    if len(sys.argv) != 3:
        print("Make sure to pass 2 configuration files while running this script.\n"
              "Command should look like this:\n"
              "python %s source_config.txt target_config.txt" % sys.argv[0])
        sys.exit()

    # get MySQL servers configuration from files
    configuration_source = get_configuration(sys.argv[1])
    configuration_target = get_configuration(sys.argv[2])

    database_source = open_connection(configuration_source)
    database_target = open_connection(configuration_target)
    print("Connections opened.")

    # get data from source database
    cursor_source = database_source.cursor(buffered=False)
    query_get_titles = "SELECT emp_no, title, from_date, to_date FROM titles"
    cursor_source.execute(query_get_titles)

    cursor_target = database_target.cursor(buffered=False)
    max_packet_size_query = "SELECT @@max_allowed_packet"  # query to get maximum allowed packet size in bytes
    cursor_target.execute(max_packet_size_query)
    max_allowed_packet_size = cursor_target.fetchone()[0]
    print("Max allowed packet size:", max_allowed_packet_size)

    packet_size = 524288    # 512KB
    # make sure if packet size
    if max_allowed_packet_size < packet_size:
        packet_size = max_allowed_packet_size

    print("Used packet size: %dKB" % int(packet_size/1024))

    query_add_title = "INSERT INTO titles (emp_no, title, from_date, to_date) VALUES (%s, %s, %s, %s)"

    prepared_inserts = []
    size_of_data = 0
    for emp_no, title, from_date, to_date in cursor_source:
        prepared_inserts.append((str(emp_no), str(title), str(from_date), str(to_date)))
        size_of_data += len(str(prepared_inserts[-1])) + 1  # +1 counts for comma dividing entries in MySQL query
        # add 300 to account for size of query_add_title and give a safety margin
        if (size_of_data + 300) > packet_size:
            write_many_db(cursor_target, query_add_title, prepared_inserts)
            prepared_inserts = []
            size_of_data = 0

    write_many_db(cursor_target, query_add_title, prepared_inserts)

    database_target.commit()

    cursor_source.close()
    cursor_target.close()

    database_source.close()
    database_target.close()
    print("Connections closed.")
    print("Program runtime %.2f seconds" % (time.time() - start_time))
