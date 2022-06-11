"""__init__.py

Parser import for ease of use
"""

from observer.utils.parsers.observer_logs import parse_log_object

from observer.utils.parsers.observer_xml import XMLMapper, XMLMapping,\
    generate_xml_mapper, parse_xml_records, get_children_by_tag, load_xml_data
