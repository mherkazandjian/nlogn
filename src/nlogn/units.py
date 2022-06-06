import pint

types_map = {
    'keyword': str,
    'text': str,
    'float': float,
    'long': int,
    'date': str
}

u = pint.UnitRegistry()
bytes_units_converter = pint.UnitRegistry(filename=None)
bytes_units_converter.define('B = []')
bytes_units_converter.define('KB = 1024 B')
bytes_units_converter.define('MB = 1024 KB')
bytes_units_converter.define('GB = 1048576 KB')


