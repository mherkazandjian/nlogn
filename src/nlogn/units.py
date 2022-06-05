import numpy
import pint

types_map = {
    'keyword': numpy.str,
    'text': numpy.str,
    'float': numpy.float64,
    'long': numpy.int64,
    'date': numpy.datetime64
}

_ureg = pint.UnitRegistry()
bytes_units_converter = pint.UnitRegistry(filename=None)
bytes_units_converter.define('KB = []')
bytes_units_converter.define('MB = 1024 KB')
bytes_units_converter.define('GB = 1048576 KB')


