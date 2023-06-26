#!/usr/bin/env python3

import csv
import snowflake.connector
import sys
import argparse


import os

CSV_FILE_NAME = 'warehouse_parameters.csv'

def connect_to_snowflake(args):
    conn = snowflake.connector.connect(
        user=args.user,
        password=args.password,
        account=args.account,
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
        role=args.role
        )
    return conn


def generate():

    parser = argparse.ArgumentParser(description='Get warehouse detils from snowflake')
    parser.add_argument('--user', help='username', required=True)
    parser.add_argument('--password', help='password', required=True)
    parser.add_argument('--account', help='account', required=True)
    parser.add_argument('--warehouse', help='warehouse', required=True)
    parser.add_argument('--database', help='database', required=True)
    parser.add_argument('--schema', help='schema', required=True)
    parser.add_argument('--out', help='output_file_path', required=True)
    parser.add_argument('--role', help='role', required=True)
    args = parser.parse_args()

    conn = connect_to_snowflake(args)
    warehouse_list = []
    complete_data = []

    cur = conn.cursor()
    cur.execute("show warehouses")
    for col1 in cur:
        warehouse_list.append(col1[0])
    for warehouse_name in warehouse_list:
        print("Preparing data for warehouse - {}\n".format(warehouse_name))
        try:
            cur.execute("show parameters for warehouse {}".format(warehouse_name))
            for col1 in cur:
                warehouse_values_list = list(col1)
                warehouse_values_list.insert(0, warehouse_name)
                complete_data.append(warehouse_values_list)
        except:
            print("Failed to read the warehouse parameters for warehouse {}. Grant warehouse access to the current user otherwise warehouse parameters skipped.".format(warehouse_name))
    filename = '{}/{}'.format(args.out, CSV_FILE_NAME)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(complete_data)
    
    print("Done please check te output at {}".format(filename))


if __name__ == '__main__':
    generate()
