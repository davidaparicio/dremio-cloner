import sys

import sqlglot
from sqlglot import exp
import sqlparse

from DremioFile import DremioFile
from DremioClonerConfig import DremioClonerConfig
import json
import uuid
import os
import re

reserved_words = ['abs', 'asc', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array', 'array_max_cardinality',
                  'as', 'asensitive', 'asymmetric', 'at', 'atomic', 'authorization', 'avg', 'begin', 'begin_frame',
                  'begin_partition', 'between', 'bigint', 'binary', 'bit', 'blob', 'boolean', 'both', 'by', 'call',
                  'called', 'cardinality', 'cascaded', 'case', 'cast', 'ceil', 'ceiling', 'char', 'char_length',
                  'character', 'character_length', 'check', 'classifier', 'clob', 'close', 'coalesce', 'collate',
                  'collect', 'column', 'commit', 'condition', 'connect', 'constraint', 'contains', 'convert', 'corr',
                  'corresponding', 'count', 'covar_pop', 'covar_samp', 'create', 'cross', 'cube', 'cume_dist', 'current',
                  'current_catalog', 'current_date', 'current_default_transform_group', 'current_path', 'current_role',
                  'current_row', 'current_schema', 'current_time', 'current_timestamp', 'current_transform_group_for_type',
                  'current_user', 'cursor', 'cycle', 'data', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default',
                  'define', 'delete', 'dense_rank', 'deref', 'desc', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct',
                  'double', 'drop', 'dynamic', 'each', 'element', 'else', 'empty', 'end', 'end-exec', 'end_frame', 'end_partition',
                  'equals', 'escape', 'every', 'except', 'exec', 'execute', 'exists', 'exp', 'explain', 'extend', 'external',
                  'extract', 'false', 'fetch', 'filter', 'first_value', 'float', 'floor', 'for', 'foreign', 'frame_row', 'free',
                  'from', 'full', 'function', 'fusion', 'get', 'global', 'grant', 'group', 'grouping', 'groups', 'having',
                  'hold', 'hour', 'identity', 'if', 'import', 'in', 'index', 'indicator', 'initial', 'inner', 'inout', 'insensitive',
                  'insert', 'int', 'integer', 'intersect', 'intersection', 'interval', 'into', 'is', 'join', 'key', 'lag', 'language',
                  'large', 'last_value', 'lateral', 'lead', 'leading', 'left', 'like', 'like_regex', 'limit', 'ln', 'local',
                  'localtime', 'localtimestamp', 'lower', 'match', 'matches', 'match_number', 'match_recognize', 'max',
                  'measures', 'member', 'merge', 'method', 'min', 'minute', 'mod', 'modifies', 'module', 'month', 'more',
                  'multiset', 'name', 'national', 'natural', 'nchar', 'nclob', 'new', 'next', 'no', 'none', 'normalize', 'not',
                  'nth_value', 'ntile', 'null', 'nullif', 'numeric', 'occurrences_regex', 'octet_length', 'of', 'offset',
                  'old', 'omit', 'on', 'one', 'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay',
                  'parameter', 'partition', 'partitions', 'pattern', 'per', 'percent', 'percentile_cont', 'percentile_disc', 'percent_rank',
                  'period', 'permute', 'portion', 'position', 'position_regex', 'power', 'precedes', 'precision', 'prepare',
                  'prev', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive', 'ref', 'references', 'referencing',
                  'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy',
                  'regr_syy', 'release', 'reset', 'result', 'return', 'returns', 'revoke', 'right', 'rollback', 'rollup',
                  'row', 'row_number', 'rows', 'running', 'savepoint', 'scope', 'scroll', 'search', 'second', 'seek',
                  'select', 'sensitive', 'session_user', 'set', 'minus', 'show', 'similar', 'skip', 'smallint', 'some',
                  'specific', 'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
                  'stddev_pop', 'stddev_samp', 'stream', 'submultiset', 'subset', 'substring', 'substring_regex', 'succeeds',
                  'sum', 'symmetric', 'system', 'system_time', 'system_user', 'table', 'tablesample', 'text', 'then', 'time',
                  'timestamp', 'timezone_hour', 'timezone_minute', 'tinyint', 'to', 'trailing', 'translate', 'translate_regex',
                  'translation', 'treat', 'trigger', 'trim', 'trim_array', 'true', 'truncate', 'uescape', 'union', 'unique',
                  'unknown', 'unnest', 'update', 'upper', 'upsert', 'user', 'using', 'value', 'values', 'value_of', 'var_pop',
                  'var_samp', 'varbinary', 'varchar', 'varying', 'versioning', 'when', 'whenever', 'where', 'width_bucket',
                  'window', 'with', 'within', 'without', 'year']

def path_matches(match_path, resource_path):
    if len(match_path) > len(resource_path):
        return False
    for i in range(0, len(match_path)):
        if match_path[i] != resource_path[i]:
            return False
    return True

def path_matches_sqlcontext(match_path, resource_path):
    for i in range(0, min(len(match_path), len(resource_path))):
        if match_path[i] != resource_path[i]:
            return False
    return True

def rebuild_path(migration, resource_path):
    src_elem_count = len(migration['srcPath'])
    new_path_end =  resource_path[src_elem_count:]
    return migration['dstPath'] + new_path_end

def rebuild_path_sqlcontext(migration, resource_path):
    if len(migration['srcPath']) >= len(resource_path):
        return migration['dstPath'][:len(resource_path)]
    return rebuild_path(migration, resource_path)

def replace_slashed_comments(sql):
    lines = []
    multiline_comment_open = False
    for line in sql.splitlines():
        stripped = line.strip()
        multiline_comment_open_idx = stripped.find('/*')
        if multiline_comment_open:
            multiline_comment_close_idx = stripped.find('*/')
            if multiline_comment_close_idx == -1:
                lines.append('-- ' + stripped)
            else:
                before = stripped[:multiline_comment_close_idx]
                after = stripped[multiline_comment_close_idx + 2:]
                if len(before) > 0:
                    # within comment
                    lines.append('-- ' + before)
                if len(after) > 0:
                    # append code
                    lines.append(after)
                multiline_comment_open = False
        elif stripped.startswith('// '):
            lines.append('-- ' + stripped[3:])
        elif stripped.startswith('//'):
            lines.append('-- ' + stripped[2:])
        elif stripped.startswith('-- '):
            lines.append(stripped)
        elif stripped.startswith('--'):
            lines.append('-- ' + stripped[2:])
        elif multiline_comment_open_idx != -1:
            before = stripped[:multiline_comment_open_idx]
            after = stripped[multiline_comment_open_idx+2:]
            if len(before) > 0:
                # append code
                lines.append(before)
            if len(after) > 0:
                # within comment
                lines.append('-- ' + after)
            multiline_comment_close_idx = stripped.find('*/')
            if multiline_comment_close_idx == -1:
                # not closed in same line
                multiline_comment_open = True
            else:
                before = stripped[:multiline_comment_close_idx]
                after = stripped[multiline_comment_close_idx + 2:]
                if len(before) > 0:
                    # within comment
                    lines.append('-- ' + before)
                if len(after) > 0:
                    # append code
                    lines.append(after)
        elif ' //' in stripped:
            lines.append(stripped.replace(' //', ' --'))
        else:
            lines.append(stripped)
    return '\n'.join(lines)


def on_clause_replace(clause, src_path, dst_path, vds_path_str, log_text):
    if isinstance(clause, dict):
        for v in clause.values():
            if isinstance(v, list):
                # TODO probably map later if required -> List then check strings
                continue
            on_clause_replace(v, src_path, dst_path, vds_path_str, log_text)
    elif isinstance(clause, list):
        for idx, item in enumerate(clause):
            if isinstance(item, dict):
                on_clause_replace(item, src_path, dst_path, vds_path_str, log_text)
            elif isinstance(item, str):
                if item.lower().startswith(src_path.lower()):
                    _newvalue = dst_path + item[len(src_path):]
                    clause[idx] = _newvalue
                    print(log_text + ' - Matching VDS SQL ON CLAUSE (' + (vds_path_str) + '): ' + item + ' -> ' + _newvalue)

def migrate_sql_with_string_replacement(sql_text, source_migrations, vds_path_str):
    """
    Migrate SQL using string replacement to preserve ALL formatting including:
    - Original line breaks (\r\n, \n, etc.)
    - Comments (both -- and /* */)
    - Whitespace and indentation
    - Everything else exactly as it was
    """
    if not sql_text:
        return sql_text
    
    migrated_sql = sql_text
    replacements_made = []
    
    for migration in source_migrations:
        src_path = '.'.join(migration['srcPath'])
        dst_path = '.'.join(migration['dstPath'])
        
        # Find all occurrences (case-insensitive) and replace them
        # Using regex with word boundaries to avoid partial matches
        pattern = re.escape(src_path)
        
        # Count occurrences for logging
        matches = list(re.finditer(pattern, migrated_sql, re.IGNORECASE))
        
        if matches:
            # Replace all occurrences
            migrated_sql = re.sub(pattern, dst_path, migrated_sql, flags=re.IGNORECASE)
            replacements_made.append({
                'src': src_path,
                'dst': dst_path,
                'count': len(matches)
            })
    
    # Log replacements
    for replacement in replacements_made:
        print(f"String Replacement - VDS ({vds_path_str}): {replacement['src']} -> {replacement['dst']} ({replacement['count']} occurrences)")
    
    return migrated_sql

def replace_table_names(parsed_sql_obj, vds_path, src_path, dst_path, log_text):
    """
    Replace table names in sqlglot parsed SQL object
    """
    if not isinstance(parsed_sql_obj, (exp.Expression, dict)):
        return
    
    # If it's a sqlglot expression, traverse it
    if isinstance(parsed_sql_obj, exp.Expression):
        for node in parsed_sql_obj.walk():
            if isinstance(node, exp.Table):
                table_name = node.sql(dialect="dremio")
                # Remove quotes for comparison
                clean_table = table_name.replace('"', '')
                if clean_table.lower().startswith(src_path.lower().replace('\\', '')):
                    new_table = dst_path.replace('\\', '') + clean_table[len(src_path.replace('\\', '')):]
                    vds_path_str = '.'.join(vds_path)
                    print(log_text + ' - Matching VDS SQL (' + vds_path_str + '): ' + clean_table + ' -> ' + new_table)
                    # Update the table name
                    parts = new_table.split('.')
                    # Set the table name (last part)
                    node.set("this", exp.Identifier(this=parts[-1]))
                    # Handle multi-part schemas properly
                    if len(parts) == 2:
                        # Just schema.table
                        node.set("db", exp.Identifier(this=parts[0]))
                        node.args.pop("catalog", None)
                    elif len(parts) == 3:
                        # catalog.schema.table
                        node.set("catalog", exp.Identifier(this=parts[0]))
                        node.set("db", exp.Identifier(this=parts[1]))
                    elif len(parts) > 3:
                        # catalog.schema1.schema2...table
                        node.set("catalog", exp.Identifier(this=parts[0]))
                        node.set("db", exp.Identifier(this='.'.join(parts[1:-1])))
    # If it's a dict (from original parsed format), handle it
    elif isinstance(parsed_sql_obj, dict):
        if 'from' in parsed_sql_obj:
            handle_from_clause(parsed_sql_obj['from'], src_path, dst_path, vds_path, log_text)
        if 'join' in parsed_sql_obj:
            handle_join_clause(parsed_sql_obj['join'], src_path, dst_path, vds_path, log_text)
        if 'on' in parsed_sql_obj:
            on_clause_replace(parsed_sql_obj['on'], src_path, dst_path, '.'.join(vds_path), log_text)
        # Recursively handle nested structures
        for key, value in parsed_sql_obj.items():
            if isinstance(value, (dict, list)):
                replace_table_names(value, vds_path, src_path, dst_path, log_text)
    elif isinstance(parsed_sql_obj, list):
        for item in parsed_sql_obj:
            replace_table_names(item, vds_path, src_path, dst_path, log_text)

def handle_from_clause(from_clause, src_path, dst_path, vds_path, log_text):
    if isinstance(from_clause, str):
        if from_clause.lower().startswith(src_path.lower().replace('\\', '')):
            return dst_path.replace('\\', '') + from_clause[len(src_path.replace('\\', '')):]
    elif isinstance(from_clause, dict):
        if 'value' in from_clause and isinstance(from_clause['value'], str):
            if from_clause['value'].lower().startswith(src_path.lower().replace('\\', '')):
                _newvalue = dst_path.replace('\\', '') + from_clause['value'][len(src_path.replace('\\', '')):]
                vds_path_str = '.'.join(vds_path)
                print(log_text + ' - Matching VDS SQL (' + vds_path_str + '): ' + from_clause['value'] + ' -> ' + _newvalue)
                from_clause['value'] = _newvalue
    elif isinstance(from_clause, list):
        for item in from_clause:
            handle_from_clause(item, src_path, dst_path, vds_path, log_text)

def handle_join_clause(join_clause, src_path, dst_path, vds_path, log_text):
    if isinstance(join_clause, dict):
        if 'value' in join_clause and isinstance(join_clause['value'], str):
            if join_clause['value'].lower().startswith(src_path.lower().replace('\\', '')):
                _newvalue = dst_path.replace('\\', '') + join_clause['value'][len(src_path.replace('\\', '')):]
                vds_path_str = '.'.join(vds_path)
                print(log_text + ' - Matching VDS SQL (' + vds_path_str + '): ' + join_clause['value'] + ' -> ' + _newvalue)
                join_clause['value'] = _newvalue
        # Handle nested joins
        for key, value in join_clause.items():
            if isinstance(value, (dict, list)) and key != 'value':
                replace_table_names(value, vds_path, src_path, dst_path, log_text)
    elif isinstance(join_clause, list):
        for item in join_clause:
            handle_join_clause(item, src_path, dst_path, vds_path, log_text)

def should_quote(identifier, dremio_data):
    """
    Determine if an identifier should be quoted
    """
    if identifier.lower() in reserved_words:
        return True
    # Check if it contains special characters
    if not identifier.replace('_', '').replace('-', '').isalnum():
        return True
    # Check if it starts with a number
    if identifier and identifier[0].isdigit():
        return True
    return False

def write_error_files(destination_file, vds, error_message, error_idx):
    """
    Write error information to files
    """
    # Create error directory next to destination file
    if destination_file:
        error_dir = os.path.join(os.path.dirname(destination_file), 'errors')
    else:
        error_dir = 'errors'
    os.makedirs(error_dir, exist_ok=True)
    
    error_file = os.path.join(error_dir, f'error_{error_idx}.txt')
    with open(error_file, 'w') as f:
        f.write(f"VDS Path: {'.'.join(vds['path'])}\n")
        f.write(f"Error: {error_message}\n")
        f.write(f"\nOriginal SQL:\n{vds.get('sql', 'N/A')}\n")
        if 'parsedSql' in vds:
            f.write(f"\nParsed SQL Object:\n{json.dumps(vds['parsedSql'], indent=2)}\n")

def parse_sql(sql_text):
    """
    Parse SQL using sqlglot with Dremio dialect
    """
    try:
        # Try parsing with Dremio dialect
        parsed = sqlglot.parse_one(sql_text, dialect="dremio")
        return parsed
    except Exception as e:
        # Fallback to default dialect
        try:
            parsed = sqlglot.parse_one(sql_text)
            return parsed
        except Exception as e2:
            raise Exception(f"Failed to parse SQL: {str(e)}, Fallback error: {str(e2)}")

def format_sql(parsed_sql, dremio_data):
    """
    Format parsed SQL back to string using sqlglot
    """
    if isinstance(parsed_sql, exp.Expression):
        # Use sqlglot to generate SQL
        sql = parsed_sql.sql(dialect="dremio", pretty=True)
        return sql
    else:
        # Fallback for dict-based parsing (shouldn't happen with sqlglot)
        return str(parsed_sql)

def main():
    args = sys.argv
    if len(args) < 2:
        print('Please specify config json path')
        exit(1)
    
    # Load config directly from JSON file
    with open(sys.argv[1], 'r') as f:
        config_data = json.load(f)
    
    # Extract paths and migrations from config
    source_file = config_data.get('sourceFile')
    destination_file = config_data.get('destinationFile')
    sourceMigrations = config_data.get('sourceMigrations', [])
    spaceFolderMigrations = config_data.get('spaceFolderMigrations', [])
    
    # Parse sourceMigrations - convert dot-separated paths to list format
    for migration in sourceMigrations:
        if 'srcPath' in migration and len(migration['srcPath']) == 1 and '.' in migration['srcPath'][0]:
            migration['srcPath'] = migration['srcPath'][0].split('.')
        if 'dstPath' in migration and len(migration['dstPath']) == 1 and '.' in migration['dstPath'][0]:
            migration['dstPath'] = migration['dstPath'][0].split('.')
    
    if not source_file:
        print('ERROR: sourceFile not specified in config')
        exit(1)
    
    # Create a simple config object for DremioFile
    class SimpleConfig:
        def __init__(self, source_filename, target_filename):
            self.source_filename = source_filename
            self.target_filename = target_filename
            self.source_directory = None
            self.target_directory = None
            self.source_endpoint = None
            self.cloner_conf_json = []
            self.home_process_mode = 'skip'
            self.source_process_mode = 'skip'
            self.space_process_mode = 'process'
            self.folder_process_mode = 'process'
            self.pds_process_mode = 'process'
            self.vds_process_mode = 'process'
            self.reflection_process_mode = 'skip'
            self.user_process_mode = 'skip'
            self.group_process_mode = 'skip'
            self.wlm_queue_process_mode = 'skip'
            self.wlm_rule_process_mode = 'skip'
            self.tag_process_mode = 'skip'
            self.wiki_process_mode = 'skip'
            self.udf_process_mode = 'skip'
            self.target_separate_sql_and_metadata_files = False
            self.container_filename = '__CONTAINER__.json'
            self.dremio_conf_filename = 'dremio_cloner.json'
    
    config = SimpleConfig(source_file, destination_file)
    file = DremioFile(config)
    dremio_data = file.read_dremio_environment()

    error_idx = 0
    # Check if we need to repair some unreferenced folders after space removal
    # TODO: we might need to remove a source or so but we have dependencies
    #  which usually still exist and maybe work but are not reachable via UI
    #  currently we just re-append them to the default space or the first folder existing
    #  which contains a root node
    unreferenced_folders = find_unreferenced_folders(dremio_data)

    if len(unreferenced_folders) > 0:
        print("Detected unreferenced folders - fixing ...")
        while len(unreferenced_folders) > 0:
            for unreferenced_folder in unreferenced_folders:
                parent_folder_path = unreferenced_folder['path'][:-1]
                if len(parent_folder_path) == 1:
                    # space should be there
                    parent_space = None
                    for space in dremio_data.spaces:
                        if space['name'] == parent_folder_path[0]:
                            parent_space = space
                            break
                    if parent_space == None:
                        print("ERROR - Space not found: " + parent_folder_path[0])
                        exit(1)
                    else:
                        print("Appending folder " + ('.'.join(unreferenced_folder['path'])) + " to space " + parent_space['name'])
                        parent_space['children'].append({
                            'id': unreferenced_folder['id'],
                            'containerType': 'FOLDER',
                            'type': 'CONTAINER',
                            'path': unreferenced_folder['path']
                        })
                else:
                    parent_folder = None
                    for folder in dremio_data.folders:
                        if folder['path'] == parent_folder_path:
                            parent_folder = folder
                            break
                    if parent_folder == None:
                        print("No existing parent folder found, creating one: " + ('.'.join(parent_folder_path)))
                        parent_folder = {
                            'id': str(uuid.uuid4()),
                            'accessControlList': {'roles': []},
                            'entityType': 'folder',
                            'path': parent_folder_path,
                            'children': []
                        }
                        # needs to go to first position otherwise dependency creation could fail
                        dremio_data.folders.insert(0, parent_folder)
                    print("Appending folder " + ('.'.join(unreferenced_folder['path'])) + " to folder " + ('.'.join(parent_folder['path'])))
                    parent_folder['children'].append({
                        'id': unreferenced_folder['id'],
                        'containerType': 'FOLDER',
                        'type': 'CONTAINER',
                        'path': unreferenced_folder['path']
                    })
            unreferenced_folders = find_unreferenced_folders(dremio_data)

    # Apply spaceFolderMigrations first (for spaces, folders, and VDS paths)
    if spaceFolderMigrations is not None and len(spaceFolderMigrations) > 0:
        print("Applying space/folder migrations...")
        for migration in spaceFolderMigrations:
            # Migrate space names
            for space in dremio_data.spaces:
                if 'path' in space and path_matches(migration['srcPath'], space['path']):
                    oldpath = space['path']
                    space['path'] = rebuild_path(migration, oldpath)
                    if 'name' in space:
                        space['name'] = space['path'][-1]
                    print("Space/Folder Migration - Space path: " + '.'.join(oldpath) + " -> " + '.'.join(space['path']))
                elif 'name' in space and len(migration['srcPath']) == 1 and space['name'] == migration['srcPath'][0]:
                    # Handle spaces that only have 'name' field
                    space['name'] = migration['dstPath'][0]
                    if 'path' not in space:
                        space['path'] = [space['name']]
                    print("Space/Folder Migration - Space name: " + migration['srcPath'][0] + " -> " + space['name'])
            
            # Migrate folder paths
            for folder in dremio_data.folders:
                if 'path' in folder and path_matches(migration['srcPath'], folder['path']):
                    oldpath = folder['path']
                    folder['path'] = rebuild_path(migration, oldpath)
                    print("Space/Folder Migration - Folder path: " + '.'.join(oldpath) + " -> " + '.'.join(folder['path']))
            
            # Migrate VDS paths
            for vds in dremio_data.vds_list:
                if 'path' in vds and path_matches(migration['srcPath'], vds['path']):
                    oldpath = vds['path']
                    vds['path'] = rebuild_path(migration, oldpath)
                    print("Space/Folder Migration - VDS path: " + '.'.join(oldpath) + " -> " + '.'.join(vds['path']))
            
            # Update children references in spaces
            for space in dremio_data.spaces:
                for child in space.get('children', []):
                    if 'path' in child and path_matches(migration['srcPath'], child['path']):
                        oldpath = child['path']
                        child['path'] = rebuild_path(migration, oldpath)
                        print("Space/Folder Migration - Space child reference: " + '.'.join(oldpath) + " -> " + '.'.join(child['path']))
            
            # Update children references in folders
            for folder in dremio_data.folders:
                for child in folder.get('children', []):
                    if 'path' in child and path_matches(migration['srcPath'], child['path']):
                        oldpath = child['path']
                        child['path'] = rebuild_path(migration, oldpath)
                        print("Space/Folder Migration - Folder child reference: " + '.'.join(oldpath) + " -> " + '.'.join(child['path']))

    # NEW APPROACH: Use string replacement instead of parsing/regenerating SQL
    # This preserves ALL original formatting including \r\n, comments, etc.
    print("Migrating VDS SQL using string replacement (preserves formatting)...")
    
    new_vds_list = []
    for vds in dremio_data.vds_list:
        try:
            vds_path_str = '.'.join(vds['path'])
            
            if 'sql' in vds and vds['sql']:
                # Apply string replacement migrations to preserve original formatting
                if sourceMigrations and len(sourceMigrations) > 0:
                    vds['sql'] = migrate_sql_with_string_replacement(
                        vds['sql'], 
                        sourceMigrations,
                        vds_path_str
                    )
                print("MIGRATED SQL (preserved formatting) for: " + vds_path_str)
            else:
                print("NO SQL to migrate for: " + vds_path_str)
            
            new_vds_list.append(vds)
        except Exception as e:
            write_error_files(destination_file, vds, str(e), error_idx)
            print("ERROR: Unable to migrate SQL for: " + '.'.join(vds['path']))
            error_idx += 1
    
    dremio_data.vds_list = new_vds_list

    # Update sqlContext for VDSs
    if sourceMigrations is not None and len(sourceMigrations) > 0:
        for migration in sourceMigrations:
            for vds in dremio_data.vds_list:
                if 'sqlContext' in vds and path_matches(migration['srcPath'], vds['sqlContext']):
                    oldpath = vds['sqlContext']
                    vds['sqlContext'] = rebuild_path(migration, oldpath)
                    print("Source Migration - Matching VDS SQL Context (" + '.'.join(vds['path']) + "): " + ('.'.join(oldpath)) + " -> " + ('.'.join(vds['sqlContext'])))
    
    # Update vds_parents
    if sourceMigrations is not None and len(sourceMigrations) > 0:
        for migration in sourceMigrations:
            for vds_parent in dremio_data.vds_parents:
                src_path = '/'.join(migration['srcPath'])
                dst_path = '/'.join(migration['dstPath'])
                parents = []
                for parent in vds_parent['parents']:
                    if parent.lower().startswith(src_path.lower()):
                        print("Matching vds_parent: " + ('.'.join(vds_parent['path'])) + " - changed dependency: " + src_path + " -> " + dst_path)
                        parents.append(dst_path + parent[len(src_path):])
                    else:
                        parents.append(parent)
                vds_parent['parents'] = parents
    
    # Also apply spaceFolderMigrations to vds_parents
    if spaceFolderMigrations is not None and len(spaceFolderMigrations) > 0:
        for migration in spaceFolderMigrations:
            for vds_parent in dremio_data.vds_parents:
                # Update the vds_parent path itself
                if 'path' in vds_parent and path_matches(migration['srcPath'], vds_parent['path']):
                    oldpath = vds_parent['path']
                    vds_parent['path'] = rebuild_path(migration, oldpath)
                    print("Space/Folder Migration - vds_parent path: " + '.'.join(oldpath) + " -> " + '.'.join(vds_parent['path']))
                
                # Update parent dependencies
                if 'parents' in vds_parent:
                    src_path = '/'.join(migration['srcPath'])
                    dst_path = '/'.join(migration['dstPath'])
                    parents = []
                    for parent in vds_parent['parents']:
                        if parent.lower().startswith(src_path.lower()):
                            print("Space/Folder Migration - vds_parent dependency: " + ('.'.join(vds_parent.get('path', ['unknown']))) + " - changed: " + src_path + " -> " + dst_path)
                            parents.append(dst_path + parent[len(src_path):])
                        else:
                            parents.append(parent)
                    vds_parent['parents'] = parents

    new_pds_list = []
    for pds in dremio_data.pds_list:
        if 'path' not in pds:
            continue
            
        pds_path = pds['path']
        migrated = False
        
        # First check spaceFolderMigrations
        if spaceFolderMigrations is not None and len(spaceFolderMigrations) > 0:
            for migration in spaceFolderMigrations:
                if path_matches(migration['srcPath'], pds_path):
                    oldpath = pds_path
                    pds['path'] = rebuild_path(migration, oldpath)
                    print("Space/Folder Migration - Moved PDS: " + '.'.join(oldpath) + ' -> ' + '.'.join(pds['path']))
                    migrated = True
                    break
        
        # Then check sourceMigrations
        if not migrated and sourceMigrations is not None and len(sourceMigrations) > 0:
            for migration in sourceMigrations:
                if path_matches(migration['srcPath'], pds_path):
                    oldpath = pds_path
                    pds['path'] = rebuild_path(migration, oldpath)
                    print("Source Migration - Moved PDS: " + '.'.join(oldpath) + ' -> ' + '.'.join(pds['path']))
                    migrated = True
                    # only one migration per pds path
                    break
        
        if migrated:
            new_pds_list.append(pds)

    dremio_data.pds_list = new_pds_list


    dremio_data.sources = []
    dremio_data.homes = []

    # Save using the same file object that was created at the beginning
    file.save_dremio_environment(dremio_data)
    if destination_file:
        print(f"Saved migrated data to: {destination_file}")
    else:
        print(f"Saved migrated data to: {source_file}")


def find_unreferenced_folders(dremio_data):
    unreferenced_folders = []
    for folder in dremio_data.folders:
        found = False
        for space in dremio_data.spaces:
            for child in space['children']:
                if child['path'] == folder['path']:
                    found = True
        for folder2 in dremio_data.folders:
            for child in folder2['children']:
                if child['path'] == folder['path']:
                    found = True
        if not found:
            unreferenced_folders.append(folder)
    return unreferenced_folders

def find_unreferenced_vds(dremio_data):
    unreferenced_vds = []
    for vds in dremio_data.vds_list:
        found = False
        for space in dremio_data.spaces:
            for child in space['children']:
                if child['path'] == vds['path']:
                    found = True
        for folder2 in dremio_data.folders:
            for child in folder2['children']:
                if child['path'] == vds['path']:
                    found = True
        if not found:
            unreferenced_vds.append(vds)
    return unreferenced_vds


if __name__ == "__main__":
	main()
