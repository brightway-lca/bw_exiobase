from . import CONVERTED_DATA_DIR
from .version_config import VERSIONS
from pathlib import Path
import bz2
import csv
import itertools
import tarfile


def convert_xlsb(workbook, worksheet):
    import pyxlsb

    wb = pyxlsb.open_workbook(workbook)
    sheet = wb.get_sheet(worksheet)

    directory = CONVERTED_DATA_DIR / Path(workbook).name.replace(".xlsb", "")
    directory.mkdir(mode=0o755, exist_ok=True)

    with bz2.open(directory / (worksheet + ".csv.bz2"), "wt", newline="") as compressed:
        writer = csv.writer(compressed)
        for i, row in enumerate(sheet.rows()):
            writer.writerow([c.v for c in row])
            if i and not i % 250:
                print(f"Row {i}")


def convert_exiobase(dirpath, version="3.3.17 hybrid"):
    dirpath = Path(dirpath)
    for obj in iterate_worksheets(version):
        print("Worksheet: {}".format(obj["worksheet"]))
        convert_xlsb(dirpath / (obj["filename"] + ".xlsb"), obj["worksheet"])


def package_exiobase(version="3.3.17 hybrid"):
    with tarfile.open(CONVERTED_DATA_DIR / "exiobase-{}.tar".format(version.replace(" ", "-")), "w") as tar:
        for pth in CONVERTED_DATA_DIR.iterdir():
            tar.add(CONVERTED_DATA_DIR / pth, arcname=str(pth))


def labels_for_compressed_data(filepath, row_offset=None, col_offset=None):
    row_offset_guess, col_offset_guess = get_offsets(filepath)
    if row_offset is None:
        row_offset = row_offset_guess
    if col_offset is None:
        col_offset = col_offset_guess

    row_labels, col_labels = [], []

    with bz2.open(filepath, "rt") as f:
        reader = csv.reader(f)
        col_labels = list(
            itertools.zip_longest(
                *[row[col_offset:] for _, row in zip(range(row_offset), reader)]
            )
        )
        row_labels = [row[:col_offset] for row in reader]

    return row_labels, col_labels


def iterate_worksheets(version, label=None):
    if label is None:
        return (elem for obj in VERSIONS[version].values() for elem in obj)
    else:
        return iter(VERSIONS[version][label])


def get_labels_for_exiobase(version="3.3.17 hybrid"):
    return {
        obj["worksheet"]: labels_for_compressed_data(
            CONVERTED_DATA_DIR / obj["filename"] / (obj["worksheet"] + ".csv.bz2"),
            obj["row offset"],
            obj["col offset"],
        )
        for obj in iterate_worksheets(version)
    }


def get_data_iterator(filepath, row_offset, col_offset):
    with bz2.open(filepath, "rt") as f:
        for i, row in enumerate(csv.reader(f)):
            for j, value in enumerate(row):
                if i >= row_offset and j >= col_offset and value and float(value) != 0:
                    yield (i - row_offset, j - col_offset, float(value))


def get_exiobase_data_iterator(version, label, worksheet=None):
    return itertools.chain(
        *[
            get_data_iterator(
                CONVERTED_DATA_DIR / obj["filename"] / (obj["worksheet"] + ".csv.bz2"),
                obj["row offset"],
                obj["col offset"],
            )
            for obj in iterate_worksheets(version, label)
            if (worksheet is None or obj["worksheet"] == worksheet)
        ]
    )


def get_all_biosphere_flows():
    labels = get_labels_for_exiobase()
    return labels["resource_act"][0] + labels["Land_act"][0] + labels["Emiss_act"][0]
