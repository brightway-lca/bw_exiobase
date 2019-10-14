from . import DATA_DIR
from pathlib import Path
import bz2
import csv
import itertools


def convert_xlsb(workbook, worksheet):
    import pyxlsb

    wb = pyxlsb.open_workbook(workbook)
    sheet = wb.get_sheet(worksheet)

    directory = (DATA_DIR / Path(workbook).name.replace(".xlsb", ""))
    directory.mkdir(mode=0o755, exist_ok=True)

    with bz2.open(directory / (worksheet + ".csv.bz2"), "wt", newline='') as compressed:
        writer = csv.writer(compressed)
        for i, row in enumerate(sheet.rows()):
            writer.writerow([c.v for c in row])
            if i and not i % 250:
                print(f"Row {i}")


def convert_exiobase(dirpath):
    dirpath = Path(dirpath)
    inputs = [
        ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech.xlsb", "Principal_production_vector"),
        ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech.xlsb", "HIOT"),
        ("MR_HIOT_2011_v3_3_17_extensions.xlsb", 'resource_act'),
        ("MR_HIOT_2011_v3_3_17_extensions.xlsb", 'Land_act'),
        ("MR_HIOT_2011_v3_3_17_extensions.xlsb", 'Emiss_act'),
    ]

    for workbook, worksheet in inputs:
        print("Worksheet: {}".format(worksheet))
        convert_xlsb(dirpath / workbook, worksheet)


def labels_for_compressed_data(filepath, row_offset=None, col_offset=None):
    row_offset_guess, col_offset_guess = get_offsets(filepath)
    if row_offset is None:
        row_offset = row_offset_guess
    if col_offset is None:
        col_offset = col_offset_guess

    row_labels, col_labels = [], []

    with bz2.open(filepath, "rt") as f:
        reader = csv.reader(f)
        col_labels = list(itertools.zip_longest(*[row[col_offset:] for _, row in zip(range(row_offset), reader)]))
        row_labels = [row[:col_offset] for row in reader]

    return row_labels, col_labels


def get_offsets(filepath):
    empty = lambda x: x in {None, ''}

    with bz2.open(filepath, "rt") as f:
        reader = csv.reader(f)

        array = [row[:25] for _, row in zip(range(25), reader)]

    if not empty(array[0][0]):
        col_offset = row_offset = 0
    else:
        col_offset = max([j for j, value in enumerate(array[0]) if value in {None, ''}]) + 1
        row_offset = max([i for i, row in enumerate(array) if row[0] in {None, ''}]) + 1

    return row_offset, col_offset


def get_labels_for_exiobase():
    inputs = [
        ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech", "Principal_production_vector", 8, 1),
        ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech", "HIOT", 4, 5),
        ("MR_HIOT_2011_v3_3_17_extensions", 'resource_act', 4, 2),
        ("MR_HIOT_2011_v3_3_17_extensions", 'Land_act', 4, 2),
        ("MR_HIOT_2011_v3_3_17_extensions", 'Emiss_act', 4, 3),
    ]
    return {b: labels_for_compressed_data(DATA_DIR / a / (b + ".csv.bz2"), c, d) for a, b, c, d in inputs}


def get_data_iterator(filepath, row_offset, col_offset):
    with bz2.open(filepath, "rt") as f:
        for i, row in enumerate(csv.reader(f)):
            for j, value in enumerate(row):
                if i >= row_offset and j >= col_offset and value and float(value) != 0:
                    yield (i, j, float(value))


# inputs = [
#     ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech", "Principal_production_vector", 8, 1),
#     ("Exiobase_MR_HIOT_2011_v3_3_17_by_prod_tech", "HIOT", 4, 5),
#     ("MR_HIOT_2011_v3_3_17_extensions", 'resource_act', 4, 2),
#     ("MR_HIOT_2011_v3_3_17_extensions", 'Land_act', 4, 2),
#     ("MR_HIOT_2011_v3_3_17_extensions", 'Emiss_act', 4, 3),
# ]
