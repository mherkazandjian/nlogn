import pint

types_map = {
    'keyword': str,
    'text': str,
    'float': float,
    'long': int,
    'date': str
}

_ureg = pint.UnitRegistry()
bytes_units_converter = pint.UnitRegistry(filename=None)
bytes_units_converter.define('KB = []')
bytes_units_converter.define('MB = 1024 KB')
bytes_units_converter.define('GB = 1048576 KB')


