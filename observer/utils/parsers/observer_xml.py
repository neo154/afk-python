"""xml.py

Author: neo154
Version: 0.1.1
Date Modified: 2022-11-15

Parser for XML parsing using the defused XML library and a few custom make parsers
"""

from logging import Logger
from typing import Any, Dict, List, Literal, Union
from xml.dom.minidom import Document, Element, Node

from defusedxml import minidom
from pandas import to_datetime

from observer.storage.models.storage_models import StorageLocation


class XMLMappingError(Exception):
    """Exceptions for XMLMapping and check"""

    def __init__(self, xpath: str, error_type: str="Unknown mapping error") -> None:
        self.message = f"Mapping invalid for {xpath}, {error_type}"
        super().__init__(self.message)

class XMLParsingError(Exception):
    """Exceptions for XML parsing actions"""

    def __init__(self, message: str) -> None:
        self.message = f'XML Parsing issue, {message}'
        super().__init__(self.message)

class XMLMapping(dict):
    """
    Singular mapping object in for parsing out XML values

    :attr xpath: String that identifies direct path to node that contines value(s) to parse
    :attr name: Name of the datapoint in resulting dictionary entry
    :attr attribute_name: Name of attribute in the node, not sub_element and not node value
    :attr sub_elem_xpath: Xpath of subelements if list is being created or downloaded
    :attr datetime_fmt: Datetime formatter if parse_type is datetime
    :attr utc: Indicator if this datetime value is in UTC timezone
    :attr true_vals: List of values that would map to True after being parsed
    """

    def __init__(self, xpath:str, name:str,
            parse_type: Literal['str', 'int', 'float', 'datetime', 'bool', 'list']='str',
            attribute_name: str=None, sub_elem_tag_name: str=None, datetime_fmt: str=None,
            utc:bool=False, true_vals: List[str]=None) -> None:
        super().__init__()
        self['xpath'] = xpath                   # Path to node with particular name
        self['name'] = name                     # Name in the extracted dictionary
        self['parse_type'] = parse_type         # Type to parse it to
        if parse_type == 'datetime':
            if datetime_fmt is None:
                raise XMLMappingError(xpath, 'Datetime type but no datetime format given')
            # Should include datetime fmt test perhaps?
            self['datetime_fmt'] = datetime_fmt
            self['utc'] = utc
        elif parse_type == 'bool':
            if true_vals is None:
                raise XMLMappingError('Bool type but no identifiers for true values given')
            self['true_vals'] = true_vals
        elif parse_type == 'list':
            if sub_elem_tag_name is None:
                raise XMLMappingError(xpath, 'List type but no sub elements identifier for list')
            self['sub_elem_tag_name'] = sub_elem_tag_name
        if attribute_name is not None:
            self['attribute_name'] = attribute_name

class XMLMapper(dict):
    """Full set of mappers for XML parsing"""

    def __init__(self, xpath:str, data_points:Union[List[dict], List[XMLMapping]], \
            child_record:dict=None):
        super().__init__()
        self['xpath'] = xpath
        if len(data_points) <= 0:
            raise RuntimeError(
                "XMLMapper invalid, XPath for records identified but no datapoints for level given"
            )
        if not isinstance(data_points[0], XMLMapping):
            data_points = [ XMLMapping(**data_point) for data_point in data_points ]
        tmp_l = []
        for data_point in data_points:
            if data_point['name'] in self:
                raise RuntimeError(f"XMLMapper invalid, Datapoint for {xpath} has repeated name")
            tmp_dict = {}
            for key, value in data_point.items():
                if key != 'name':
                    tmp_dict[key] = value
            tmp_l.append({'name': data_point['name'], 'map': tmp_dict})
        self['data_points'] = tmp_l
        if child_record is not None:
            self['child_record'] = XMLMapper(**child_record)
        # Self check to make sure there aren't multiple instances of samme name
        curr_ref = self
        full_nameset = []
        while True:
            for data_point in curr_ref['data_points']:

                if data_point['name'] in full_nameset:
                    raise RuntimeError(
                        "XMLMapper invalid, repeated name in final datastructure: "
                        f"{data_point['name']}"
                    )
                full_nameset.append(data_point['name'])
            # Recurse or just break
            if 'child_record' in curr_ref:
                curr_ref = curr_ref['child_record']
            else:
                break

def generate_xml_mapper(mapper_dict: dict):
    """
    Generates an XML mapper for parsing from a dictionary

    :param mapper_dict: Dictionary that contains mapper data and structure
    :returns: XMLMapper dictionary for parsing XML to flat structure
    """
    return XMLMapper(**mapper_dict)

def _xpath_split(xpath: str) -> List[str]:
    """
    Splits a given xpath to separate tags to traverse through

    :param xpath: String that identifies path in XML
    :returns: List of node names for path to element of interest
    """
    return [ name for name in xpath.split('/') if name!='' ]

def get_children_by_tag(current_node: Element, tag: str) -> List[Node]:
    """
    Gets direct child nodes by tag name that are direct children

    :param current_node: Element node that this is currently on
    :param tag: String that identifies name of nodes to return
    :returns: List of Nodes that match the search
    """
    return [ node for node in current_node.childNodes \
                if (isinstance(node, Element) and node.nodeName==tag) ]

def traverse_xpath(start_node: Element, xpath: str, logger: Logger=None) -> Union[Element, None]:
    """
    Going from node reference that is identified and gives a reference to element using
    xpath to traverse the node structure

    :param start_node: Element object to start from
    :param xpath: String that identifies the xpath to travel through
    :param logger: Logger object to use if one is provided
    :returns: Element in path or None object
    """
    not_warned = True
    cur_ref = start_node
    if xpath  in ['.', start_node.nodeName]:
        return cur_ref
    path_node_names = _xpath_split(xpath)
    while len(path_node_names) > 0:
        next_node = path_node_names.pop(0)
        child_nodes = get_children_by_tag(cur_ref, next_node)
        if len(child_nodes) <= 0:
            return None
        if len(child_nodes) > 1 and not_warned and logger is not None:
            logger.warning(
                "MORE THAN ONE NODE WAS FOUND WITH TAG %s, PICKED FIRST ENTRY!", next_node)
            not_warned = False
        cur_ref = child_nodes[0]
    return cur_ref

def _handle_none_data(mapping: dict) -> Any:
    """
    Handling none values in single function

    :param mapping: Dictionary that identifies how to type or handle None data
    :return: None value depending on how it is to be handled
    """
    data_type = mapping['parse_type']
    if data_type=='datetime':
        return to_datetime(None, utc=mapping['utc'])
    if data_type=='bool':
        return False
    if data_type=='list':
        return []

def parse_item(current_ref: Element, mapping: dict) -> Any:
    """
    Parses single item in XMLMapper object for an element

    :param current_ref: Element that is the current reference in XML doc
    :param mapping: Dictionary that dictates how to handle parsing of data
    :returns: Any data or None result of parsing
    """
    data_node = traverse_xpath(current_ref, mapping['xpath'])
    data_type = mapping['parse_type']
    if data_node is None:
        return _handle_none_data(mapping)
    if 'attribute_name' in mapping:
        raw_data = data_node.getAttribute(mapping['attribute_name'])
    else:
        if data_type != 'list':
            print(data_node.firstChild.data)
            raw_data = data_node.firstChild.data.strip()
            print(raw_data)
        else:
            raw_data = []
            for sub_elem in data_node.getElementsByTagName(mapping['sub_elem_tag_name']):
                if sub_elem.firstChild is not None:
                    raw_data.append(sub_elem.firstChild.data)
            return raw_data
    if raw_data is None:
        return_data = _handle_none_data(mapping)
    if data_type == 'str':
        return_data = raw_data
    if data_type == 'int':
        return_data = int(raw_data)
    if data_type == 'float':
        return_data = float(raw_data)
    if data_type == 'datetime':
        return_data = to_datetime(raw_data, format=mapping['datetime_fmt'], utc=mapping['utc'])
    if data_type == 'bool':
        return_data = raw_data in mapping['true_vals']
    return return_data

def _parse_xml_record(start_elem: Element, mapper: XMLMapper, logger: Logger=None,
        parent_data: dict=None) -> Dict:
    """
    Parses single XML record from an element, recursive if mapper is

    :param start_elem: Element that contains datapoint data that is going to be parsed out
    :param mapper: Mapper object that describes how to parse a record from XML
    :param parent_data: If child record, pass rest of data from level up
    :returns: Dictionary record with data and names
    """
    if parent_data is not None:
        ret_dict = parent_data
    else:
        ret_dict = {}
    record_elem = traverse_xpath(start_elem, mapper['xpath'], logger)
    for data_point_map in mapper['data_points']:
        ret_dict[data_point_map['name']] = parse_item(record_elem, data_point_map['map'])
    print(ret_dict)
    if 'child_record' in mapper:
        ret_dict = _parse_xml_record(record_elem, mapper['child_record'], logger, ret_dict)
    return ret_dict

def parse_xml_records(elems: Union[List[Element], Element], mapper: XMLMapper,
        logger: Logger=None, parent_data: dict=None):
    """
    Parses single XML record from an element, recursive if mapper is

    :param elems: Element or list of elements that contains datapoint data that is going to be
                    parsed out
    :param mapper: Mapper object that describes how to parse a record from XML
    :param parent_data: If child record, pass rest of data from level up
    :returns: Dictionary record with data and names
    """
    return [ _parse_xml_record(elem, mapper, logger, parent_data) for elem in elems ]

def load_xml_data(xml_doc: Union[str, StorageLocation], logger: Logger=None) -> Document:
    """
    Loads XML document into memory for parsing, mainipulation, etc

    :param xml_loc: StorageLocation to be able to read the full XML document
    :returns: XML Document
    """
    if not isinstance(xml_doc, str):
        xml_doc = xml_doc.read(logger)
    return minidom.parseString(xml_doc)
