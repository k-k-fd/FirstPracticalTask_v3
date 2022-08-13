import logging
from configparser import ConfigParser
import json
import os
import re
import math
from datetime import datetime
import csv


def read_config(conf_file):
    config = ConfigParser(interpolation=None)
    if not os.path.exists(conf_file):
        raise FileNotFoundError(conf_file, " not found")
    else:
        config.read(conf_file)
        conf_dict = {
            'input_file_name': config['IN']['INPUT_FILE_NAME'],
            'input_file_contains_fileheader': bool(
                config['IN']['INPUT_FILE_CONTAINS_FILEHEADER']),
            'input_file_contains_colheaders': bool(
                config['IN']['INPUT_FILE_CONTAINS_COLHEADERS']),
            'input_cols': dict(json.loads(config['IN']['INPUT_COLS'])),
            'file_header_pattern': config['IN']['FILE_HEADER_PATTERN'],
            'output_dt_pattern': config['OUT']['OUTPUT_DT_PATTERN'],
            'output_col_headers': list(
                config['OUT']['OUTPUT_COL_HEADERS'].split(',')),
            'output_file_path': config['OUT']['OUTPUT_FILE_PATH'],
            'output_order_by': config['OUT']['OUTPUT_ORDER_BY']
        }
        return conf_dict


def validate_input_file_control_num(in_file_name, file_hdr_pttrn):
    err_msg_no_in_file = 'Missing input file "{}"!'.format(in_file_name)
    if not os.path.exists(in_file_name):
        raise Exception(err_msg_no_in_file)
    else:
        with open(in_file_name, "r") as input_file:
            file_header = input_file.readline()
            records_in_file = len(input_file.readlines()) - 1
        if re.match(file_hdr_pttrn, file_header):
            control_numbers = re.findall(r'\d+/\d+', file_header)[0]
            records_to_check = int(control_numbers.split('/')[0])
            if records_to_check != records_in_file:
                raise Exception('Record to check: ' + str(records_to_check)
                                + '\nRecords in file: ' + str(records_in_file)
                                + '\nControl numbers not matching!')
            else:
                return True
        else:
            raise Exception('Wrong file header format!')


def msg_read_param(p_name, p_min, p_max):
    return "Enter " + p_name + " in degrees "\
        + "[between " + str(p_min) + " and " + str(p_max) + "]:"


def read_params(msg_to_read, p_min, p_max):
    param = input(msg_to_read)
    try:
        while not (p_min <= float(param) <= p_max):
            param = input(msg_to_read)
    except ValueError as e:
        raise Exception("Expecting numeric value!") from e
    return param


def read_top_n_param():
    msg = "Enter the number of the brightest objects to have in the output " \
        "(top N) [whole positive number]: "
    top_n_prm = input(msg)
    try:
        while not (int(top_n_prm) > 0):
            top_n_prm = input(msg)
    except ValueError as e:
        raise Exception("Expecting numeric value!") from e
    return top_n_prm


def read_input_file(input_file, input_file_contains_filehdr):
    in_ds = {}
    with open(input_file, "r") as input_file:
        if input_file_contains_filehdr:
            input_file_content = input_file.read().splitlines()[
                                 1:]  # skip file header
        else:
            input_file_content = input_file.read().splitlines()
        for row_num, row in enumerate(input_file_content):
            row_as_lst = row.split('\t')
            enum_row_as_lst = {idx: v for idx, v in enumerate(row_as_lst)}
            in_ds[row_num] = enum_row_as_lst
    return in_ds


def min_ras_dcl(ras0_dcl0, fovh_fovv):
    return ras0_dcl0 - (fovh_fovv / 2)


def max_ras_dcl(ras0_dcl0, fovh_fovv):
    return ras0_dcl0 + (fovh_fovv / 2)


def check_object_in_fov(ras, dcl, minras, maxras, mindecl, maxdecl):
    if minras <= ras <= maxras and mindecl <= dcl <= maxdecl:
        return True
    else:
        return False


def calc_dist(ra_1, ra_2, decl_1, decl_2):
    return math.sqrt((ra_2 - ra_1) ** 2 + (decl_2 - decl_1) ** 2)


def chk_input_cols_non_blank(lst_cols):
    for each_col in lst_cols:
        if each_col == '':
            raise Exception('Missing value!')
        else:
            continue
    return True


def process_dataset(in_ds, contains_colheaders, ra_param, decl_param,
                    fov_h_param, fov_v_param, in_cols):
    staging_ds = {}
    staging_ds_row_id = 0
    if contains_colheaders:
        del in_ds[0]
    for r_id, row in in_ds.items():
        in_cols_in = row.get(in_cols.get('source_id'))
        in_cols_ra = row.get(in_cols.get('ra_ep2000'))
        in_cols_dec = row.get(in_cols.get('dec_ep2000'))
        in_cols_b = row.get(in_cols.get('b'))
        min_ra_fovh = min_ras_dcl(ra_param, fov_h_param)
        max_ra_fovh = max_ras_dcl(ra_param, fov_h_param)
        min_dec_fovv = min_ras_dcl(decl_param, fov_v_param)
        max_dec_fovv = max_ras_dcl(decl_param, fov_v_param)
        chk_input_cols_non_blank([in_cols_in, in_cols_ra,
                                  in_cols_dec, in_cols_b])
        col_id = int(in_cols_in)
        col_ra = float(in_cols_ra)
        col_decl = float(in_cols_dec)
        col_bright = float(in_cols_b)
        if check_object_in_fov(col_ra, col_decl,
                               min_ra_fovh, max_ra_fovh,
                               min_dec_fovv, max_dec_fovv):
            col_dist = calc_dist(ra_param, col_ra, decl_param, col_decl)
            row_dict = {
                'ID': col_id,
                'RA': col_ra,
                'BRI': col_bright,
                'DIST': col_dist
            }
            staging_ds[staging_ds_row_id] = row_dict
        staging_ds_row_id += 1
    return staging_ds


def prep_final_dataset(input_ds, top_n, col):
    pivot_ds = {}
    list_keys_to_return = []
    dict_to_return = {}
    for k, v in input_ds.items():
        key_value = {k: v.get(col)}
        pivot_ds.update(key_value)
    if top_n > 1:
        max_k = max(pivot_ds, key=pivot_ds.get)
        list_keys_to_return.append(max_k)
        del pivot_ds[max_k]
        top_n -= 1
        while len(pivot_ds) > 0 and top_n != 0:
            max_k = max(pivot_ds, key=pivot_ds.get)
            list_keys_to_return.append(max_k)
            del pivot_ds[max_k]
            top_n -= 1
    k_dict_to_return = 1
    for i in list_keys_to_return:
        dict_to_return[k_dict_to_return] = list(input_ds.get(i).values())
        k_dict_to_return += 1
    return dict_to_return


def write_output_file(final_ds, outfile, out_col_hdrs):
    with open(outfile, "w", encoding='UTF8', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=out_col_hdrs)
        writer.writeheader()
        for key, value_as_list in final_ds.items():
            out_file.write(','.join([str(elem) for elem in value_as_list]))
            out_file.write('\n')


def main():
    config_file = 'config.ini'

    logging.basicConfig(filename='app.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')

    conf_dct = read_config(config_file)
    conf_input_file = conf_dct.get('input_file_name')
    conf_infile_contains_fileheader = \
        conf_dct.get('input_file_contains_fileheader')
    conf_infile_contains_colheader = \
        conf_dct.get('input_file_contains_colheaders')
    conf_in_cols = conf_dct.get('input_cols')
    conf_infile_header_pattern = conf_dct.get('file_header_pattern')
    conf_outfile_dt_pattern = conf_dct.get('output_dt_pattern')
    conf_outfile_col_headers = conf_dct.get('output_col_headers')
    conf_outfile_path = conf_dct.get('output_file_path')
    conf_outfile_order_by = conf_dct.get('output_order_by')

    validate_input_file_control_num(conf_input_file,
                                    conf_infile_header_pattern)

    ra_param = read_params(msg_read_param('RA', 0, 360), 0, 360)
    decl_param = read_params(msg_read_param('DEC', -90, 90), -90, 90)
    fov_h_param = read_params(msg_read_param('FOV_H', 0, 360), 0, 360)
    fov_v_param = read_params(msg_read_param('FOV_V', -90, 90), -90, 90)
    top_n_param = read_top_n_param()

    in_dataset = read_input_file(conf_input_file,
                                 conf_infile_contains_fileheader)

    stg_dataset = process_dataset(
        in_dataset,
        conf_infile_contains_colheader,
        float(ra_param),
        float(decl_param),
        float(fov_h_param),
        float(fov_v_param),
        conf_in_cols)

    if not stg_dataset:
        raise Exception("Output is blank!")

    final_dataset = prep_final_dataset(stg_dataset, int(top_n_param),
                                       conf_outfile_order_by)

    datetime_stamp = datetime.now().strftime(conf_outfile_dt_pattern)
    output_file = os.path.join(conf_outfile_path,
                               '{}.csv'.format(datetime_stamp))

    write_output_file(final_dataset, output_file, conf_outfile_col_headers)

    print('\nCompleted! Check "{}"'.format(output_file))


if __name__ == '__main__':
    main()
