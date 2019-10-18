from .utils import get_labels_for_exiobase, get_exiobase_data_iterator
from .version_config import VERSIONS
from brightway_projects import projects
from bw_default_backend import (
    CalculationPackage,
    Activity,
    Flow,
    Collection,
    filepath_for_processed_array,
    config,
)
from bw_migrations import migrate_data
from bw_processing import (
    dictionary_formatter,
    create_numpy_structured_array,
    MAX_SIGNED_32BIT_INT,
    create_calculation_package,
)
import bw_default_backend as backend
import numpy as np
import itertools


DTYPE = [
    ("row_value", np.uint32),
    ("col_value", np.uint32),
    ("row_index", np.uint32),
    ("col_index", np.uint32),
    ("amount", np.float32),
    ("flip", np.bool),
]


def flow_label_as_dict(row):
    return {
        "name": row[0],
        "unit": row[1],
        "categories": row[2] if len(row) > 2 else None,
        "amount": 1,
    }


def to_tuple(x):
    if isinstance(x, str):
        return (x,)
    else:
        return tuple(x)


def fill_missing_production(version, row_mapping, col_mapping, size):
    for i, j, value in get_exiobase_data_iterator(version, "technosphere"):
        yield (
            row_mapping[i],
            col_mapping[j],
            MAX_SIGNED_32BIT_INT,
            MAX_SIGNED_32BIT_INT,
            value,
            True,
        )
    production_seen = set()
    for i, j, value in get_exiobase_data_iterator(version, "production"):
        production_seen.add(j)
        yield (
            row_mapping[j],
            col_mapping[j],
            MAX_SIGNED_32BIT_INT,
            MAX_SIGNED_32BIT_INT,
            value,
            False,
        )
    # Fix empty rows in exiobase
    for i in range(size):
        if i not in production_seen:
            yield (
                row_mapping[i],
                col_mapping[i],
                MAX_SIGNED_32BIT_INT,
                MAX_SIGNED_32BIT_INT,
                1,
                False,
            )


def emissions_iterator(version, label, labels, flows_to_ids, activity_col_to_index):
    disaggregation = {
        i: list(migrate_data([flow_label_as_dict(row)], "exiobase-3-ecoinvent-3.6"))
        for i, row in enumerate(labels[label][0])
    }

    for i, j, value in get_exiobase_data_iterator(version, "biosphere", label):
        for dct in disaggregation[i]:
            if dct.get("categories") is None:
                # Resource flows not in ecoinvent, so no new category added
                continue
            try:
                yield (
                    flows_to_ids[(dct["name"], to_tuple(dct["categories"]))],
                    activity_col_to_index[j],
                    MAX_SIGNED_32BIT_INT,
                    MAX_SIGNED_32BIT_INT,
                    value * dct["amount"],
                    False,
                )
            except KeyError:
                continue


def import_exiobase(version="3.3.17 hybrid"):
    """Import Exiobase.

    Both migrations and this code are hard-coded against ecoinvent version 3.6."""
    collection_name = f"Exiobase {version}"
    if Collection.select().where(Collection.name == collection_name).count():
        print("EXIOBASE version already imported")
        return

    assert version in VERSIONS
    print("Getting row and column labels for all inputs")
    labels = get_labels_for_exiobase(version)

    data = {
        "exchanges": [],
        "characterization factors": [],
        "methods": [],
        "uncertainty types": [],
        "collections": [{"id": 1, "name": collection_name}],
        "geocollections": [{"id": 1, "name": "Exiobase {}".format(version[0])}],
    }
    locations = sorted({row[0] for row in labels["HIOT"][1]})
    location_mapping = {key: i for i, key in enumerate(locations)}
    data["locations"] = [
        {"id": location_mapping[key] + 1, "name": key, "geocollection_id": 1}
        for key in locations
    ]

    flows = [tuple(row[:2]) for row in labels["HIOT"][0]]
    flow_mapping = {key: i for i, key in enumerate(flows)}
    data["flows"] = [
        {
            "id": flow_mapping[tuple(row[:2])] + 1,
            "name": row[1],
            "unit": row[4],
            "kind": "product",
            "location_id": location_mapping[row[0]] + 1,
            "collection_id": 1,
            "code 1": row[2],
            "code 2": row[3],
        }
        for row in labels["HIOT"][0]
    ]

    activities = [tuple(row[:2]) for row in labels["HIOT"][1]]
    activity_mapping = {key: i for i, key in enumerate(activities)}
    data["activities"] = [
        {
            "id": activity_mapping[tuple(row[:2])] + 1,
            "name": row[1],
            "collection_id": 1,
            "location_id": location_mapping[row[0]] + 1,
            "reference_product_id": i + 1,
            "code 1": row[2],
            "code 2": row[3],
        }
        for i, row in enumerate(labels["HIOT"][1])
    ]

    print("Writing activity and flow data")
    backend.create(data)

    location_mapping = {o["id"]: o["name"] for o in data["locations"]}
    for item in data["activities"]:
        item["location"] = location_mapping[item["location_id"]]
    for item in data["flows"]:
        item["location"] = location_mapping[item["location_id"]]

    # Make sure our scheme guarantees uniqueness
    assert len(data["flows"]) == len(
        {(o["name"], o["location"]) for o in data["flows"]}
    )
    assert len(data["activities"]) == len(
        {(o["name"], o["location"]) for o in data["activities"]}
    )

    # Ensure square matrix
    size = len(data["activities"])
    assert size == len(data["flows"])

    rows = {(o["name"], o["location"]): i for i, o in enumerate(data["flows"])}
    cols = {(o["name"], o["location"]): i for i, o in enumerate(data["activities"])}
    flow_row_to_index = {
        rows[(a.name, a.location.name)]: a.id
        for a in Flow.select().where(
            Flow.collection == Collection.get(name=f"Exiobase {version}")
        )
    }
    activity_col_to_index = {
        cols[(a.name, a.location.name)]: a.id
        for a in Activity.select().where(
            Activity.collection == Collection.get(name=collection_name)
        )
    }

    flows_to_ids = {
        (f.name, f.categories): f.id
        for f in Flow.select().where(
            Flow.collection == Collection.get(name="ecoinvent 3.6 biosphere"))
    }

    resources = [{
        'name': f"{collection_name} technosphere",
        'data': fill_missing_production(version, flow_row_to_index, activity_col_to_index, size),
        'path': f'{collection_name}.technosphere.npy',
        'dtype': DTYPE,
        'matrix': 'technosphere',
    }, {
        'name': f"{collection_name} biosphere",
        'data': itertools.chain(
            emissions_iterator(version, "Emiss_act", labels, flows_to_ids, activity_col_to_index),
            emissions_iterator(version, "resource_act", labels, flows_to_ids, activity_col_to_index),
            emissions_iterator(version, "Land_act", labels, flows_to_ids, activity_col_to_index),
        ),
        'path': f'{collection_name}.biosphere.npy',
        'dtype': DTYPE,
        'matrix': 'biosphere',
    }]
    print("Writing matrices")
    fp = create_calculation_package(
        name=collection_name.replace(" ", "") + ".lci",
        resources=resources,
        path=config.processed_dir,
        metadata={
            "importer": "bw_exiobase.import_exiobase",
            "licenses": [{
                "name": "CC-BY-SA-4.0",
                "path": "https://creativecommons.org/licenses/by-sa/4.0/",
                "title": "Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0) ",
            }]
        },
        compress=True
    )
    CalculationPackage.create(
        filepath=fp, collection=Collection.get(name=collection_name)
    )
