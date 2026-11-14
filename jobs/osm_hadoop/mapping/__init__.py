"""The mapping engine: from a mapping file and OSM elements to table rows.

    config    the mapping file: tables, mappings, filters, columns, generalized tables
    tags      tag filters applied when elements are cached (load_all, exclude, include)
    match     which tables an element goes into
    columns   column values and their types
    geom      coordinates and geometries: points, lines, polygons, multipolygons
    writer    rows of nodes, ways and relations, and expired tiles
    expire    tile lists
    postgis   the SQL: tables, generalization, indexes, schema rotation, updates
"""
