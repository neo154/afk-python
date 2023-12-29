"""__init__.py

Parser import for ease of use
"""

from afk.utils.creds import (CredsManagerInterface, LocalCredsManager,
                             get_creds_manager, get_local_creds_manager)
from afk.utils.fs_ops import compress_file, export_df, export_json
from afk.utils.parsers import (XMLMapper, XMLMapping, analyze_logs,
                               generate_xml_mapper, get_children_by_tag,
                               load_xml_data, parse_log_object,
                               parse_xml_records)
from afk.utils.update_funcs import (git_update, pip_requirements_txt,
                                    pip_single_package)
