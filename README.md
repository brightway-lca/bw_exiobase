# bw_exiobase

Import the [EXIOBASE](https://exiobase.eu/) database into Brightway (version 3).

Currently only works with version 3.3.17, Hybrid Input-Output tables. See details on matching [EXIOBASE to Ecoinvent biosphere flows](https://github.com/brightway-lca/bw_migrations/blob/master/bw_migrations/data/exiobase-3-ecoinvent-3.6.json#L634) here.

Usage:

Download EXIOBASE ([version 3.3.17 HSUT 2011](https://exiobase.eu/index.php/data-download/exiobase3hyb)).

Install `bw_exiobase` using [conda](https://docs.conda.io/en/latest/miniconda.html):

    conda install -c conda-forge -c cmutel -c cmutel/label/nightly -c haasad bw_exiobase

Then, run the following in a Python shell or Jupyter notebook:

    import bw_default_backend as be
    import brightway_projects as p
    import brightway_ecoinvent_metadata
    from bw_exiobase import import_exiobase, convert_exiobase

    p.projects.create_project("exiobase", add_base_data=True)
    brightway_ecoinvent_metadata.add_ecoinvent_metadata()
    import_exiobase()
