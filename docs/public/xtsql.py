#!/usr/bin/env python3

# MIT License

# Copyright (c) 2023-2024 Håkan Råberg and Steven Deobald

# Copyright (c) 2024 JUXT Ltd

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import base64
from datetime import date, datetime, time
import json
import urllib.request

end_of_time = "9999-12-31T23:59:59.999999Z"

def parse_iso_datetime(date_string):
    """
    Attempts to parse a date_string with varying levels of granularity, from just a year
    to a full timestamp with microseconds, accepting both 'T' and ' ' as separators between
    date and time components.
    """
    # print(date_string)

    # Normalize the datetime string to use a space separator
    date_string = date_string.replace('T', ' ')

    # List of datetime formats in decreasing order of granularity
    # Note the use of optional parts with regex-like syntax in strptime is not possible,
    # so we list the formats explicitly.
    datetime_formats = [
        "%Y-%m-%d %H:%M:%S.%f%z",  # Full timestamp with microseconds and timezone
        "%Y-%m-%d %H:%M:%S.%fZ",   # Timestamp without timezone Z
        "%Y-%m-%d %H:%M:%S.%f",    # Timestamp without timezone
        "%Y-%m-%d %H:%M:%SZ",      # Timestamp without microseconds Z
        "%Y-%m-%d %H:%M:%S",       # Timestamp without microseconds
        "%Y-%m-%d %H:%MZ",         # Without seconds Z
        "%Y-%m-%d %H:%M",          # Without seconds
        "%Y-%m-%d %HZ",            # Only date and hour Z
        "%Y-%m-%d %H",             # Only date and hour
        "%Y-%m-%d",                # Only date
        "%Y-%m",                   # Only year and month
        "%Y"                       # Only year
    ]

    for fmt in datetime_formats:
        try:
            # Attempt to parse the string with the current format
            return datetime.strptime(date_string, fmt)
        except ValueError:
            # If parsing fails, move on to the next format
            continue

    # If no format matches, return max datetime
    return datetime.max

def remove_none_and_empty_dict_values(d):
    def is_empty_dict(val):
        if isinstance(val, dict) and not val:
            return True
        return False

    keys_to_delete = [key for key, value in d.items() if value is None or is_empty_dict(value)]
    for key in keys_to_delete:
        del d[key]

    for key, value in list(d.items()):
        if isinstance(value, dict):
            remove_none_and_empty_dict_values(value)
            if is_empty_dict(value):
                del d[key]

    return d

class AbstractXtdb:
    def _from_json_ld(self, obj):
        match obj.get('@type', None):
            case 'xt:timestamp':
                return parse_iso_datetime(obj['@value'])
            case 'xt:timestamptz':
                return parse_iso_datetime(obj['@value'].replace("[UTC]", ""))
            case 'xt:date':
                return date.fromisoformat(obj['@value'])
            case _:
                return obj.get('@graph', obj)

    def _to_json_ld(self, obj):
        match obj:
            case datetime():
                 return {'@value': datetime.isoformat(obj), '@type': 'xt:timestamp'}
            case date():
                 return {'@value': date.isoformat(obj), '@type': 'xt:date'}
            case _:
                 raise TypeError

class Xtdb(AbstractXtdb):
    """
    An XTDB client for the HTTP API
    Attributes
    ----------
    url : str
        HTTP URL of an XTDB Server
    """
    def __init__(self, url='http://localhost:3000', accept='application/json',
                 txtimeout=None):
        """
        Parameters
        ----------
        url : str, default='http://localhost:3000/'
            HTTP URL of an XTDB instance
        accept : str, default='application/json'
            Accept header content type
        """
        super().__init__()
        self.url = url
        self.accept = accept
        self.txtimeout = txtimeout

    def sql_query(self, q, p=[], m=False, accept=None, explain=None,
                  basis_at_tx_id=None, basis_at_system_time=None):
        """Executes a read-only SQL statement
        The SQL statement is sent to the `/query` endpoint at `Xtdb.url` over HTTP.
        Parameters
        ----------
        q : str
            SQL statement or query to execute
        p : array_like, default=[]
            An array of positional SQL parameters
        Raises
        ------
        TypeError
            Internal error if attempting to translate an unknown type
            to LD-JSON.
        """
        if accept is None:
            accept = self.accept
        headers = {'Accept': accept,
                   'Content-Type': 'application/json'}

        if p == []:
            args_value = []
        else:
            args_value = json.dumps(p, default=self._to_json_ld)

        payload = {"query": {"sql": q[:-1]},
                   "queryOpts": {# "args": args_value,
                                 "txTimeout": self.txtimeout,
                                 "keyFn": "SNAKE_CASE_STRING",
                                 "explain": explain,
                                 "basis": {"currentTime": basis_at_system_time,
                                           "atTx": {"txId": basis_at_tx_id,
                                                    "systemTime": basis_at_system_time}}}}

        payload = remove_none_and_empty_dict_values(payload)

        #print(payload)
        url = self.url + '/query'

        data = json.dumps(payload).encode('utf-8')
        # print(data)

        req = urllib.request.Request(url, data, headers, method='POST')
        with urllib.request.urlopen(req) as response:
            response_body = response.read().decode('utf-8')
            json_strings = response_body.strip().split('\n')
            json_array = [json.loads(json_str, object_hook=self._from_json_ld)
                          for json_str in json_strings if json_str]

            return json_array

    def sql_tx(self, q, p=[], m=False, accept=None):
        """Executes a transactional SQL statement
        The SQL statement is sent to the `/tx` endpoint at `Xtdb.url` over HTTP.
        Parameters
        ----------
        q : str
            SQL statement to execute
        p : array_like, default=[]
            An array of positional SQL parameters
        Raises
        ------
        TypeError
            Internal error if attempting to translate an unknown type
            to LD-JSON.
        """
        if accept is None:
            accept = self.accept
        headers = {'Accept': accept,
                   'Content-Type': 'application/json'}

        if p == []:
            args_value = []
        else:
            args_value = json.dumps(p, default=self._to_json_ld)

        # TODO afterTx, atTx

        payload = {"txOps":[{"sql": q[:-1]}]}

        url = self.url + '/tx'

        data = json.dumps(payload).encode('utf-8')

        req = urllib.request.Request(url, data, headers, method='POST')
        with urllib.request.urlopen(req) as response:
            response_body = response.read().decode('utf-8')
            json_strings = response_body.strip().split('\n')
            json_array = [json.loads(json_str, object_hook=self._from_json_ld)
                          for json_str in json_strings if json_str]

            return json_array

    def sql_status(self, accept=None):
        """Calls the `/status` endpoint of the connected XTDB server.
        """
        if accept is None:
            accept = self.accept
        headers = {'Accept': accept}

        url = self.url + '/status'

        req = urllib.request.Request(url, headers=headers, method='GET')
        with urllib.request.urlopen(req) as response:
            response_body = response.read().decode('utf-8')
            json_strings = response_body.strip().split('\n')
            json_array = [json.loads(json_str, object_hook=self._from_json_ld)
                          for json_str in json_strings if json_str]

            return json_array

# end of client functionality

# beginning of console functionality

"""XTDB Console
This script provides a prompt (->) at which the user can enter raw SQL
commands to send to XTDB.
"""

import cmd
import json
# import xtdb
import pprint
import urllib.error
import re
import time
# import edn_format

def markdownSimpleTable(data):
    # Ensure there is data to process
    if not data:
        return "No data provided."

    # Collect keys in order while avoiding duplicates
    seen_keys = set()
    ordered_keys = []
    for item in data:
        for key in item.keys():
            if key not in seen_keys:
                seen_keys.add(key)
                ordered_keys.append(key)

    # Calculate Padding For Each Column Based On Header and Data Length
    rowsPadding = {}
    for index, key in enumerate(ordered_keys):
        padCount = max(len(str(item.get(key, ""))) + 1 for item in data) + 2  # Adjusted for added space
        headerPadCount = len(key) + 2
        rowsPadding[index] = max(padCount, headerPadCount)

    # Render Markdown Header
    header_row = '|' + '|'.join((' ' + key).ljust(rowsPadding[index]) for index, key in enumerate(ordered_keys)) + '|'
    separator_row = '|' + '|'.join('-' * rowsPadding[index] for index in range(len(ordered_keys))) + '|'
    rows = [header_row, separator_row]

    # Render Tabular Data
    for item in data:
        row = '|' + '|'.join((' ' + str(item.get(key, ""))).ljust(rowsPadding[index]) for index, key in enumerate(ordered_keys)) + '|'
        rows.append(row)

    # Convert Tabular String Rows Into String
    tableString = "\n".join(rows)
    return tableString

def prepare_for_tabulate(data, special_key='xt$id'):
    # return data
    # Check if the special key exists in any of the dictionaries
    if any(special_key in row for row in data):
        # Sort keys alphanumerically, but put special_key first
        sorted_keys = sorted({key for row in data for key in row if key != special_key})
        sorted_keys.insert(0, special_key)  # Insert the special_key at the beginning
    else:
        # Just sort keys alphanumerically if special_key doesn't exist
        sorted_keys = sorted({key for row in data for key in row})

    # Reorder the dictionaries according to sorted_keys
    reordered_data = [{key: row.get(key) for key in sorted_keys} for row in data]

    return reordered_data

def pretty_format(item, nonestr='NULL', emptystr='\'\''):
    if isinstance(item, dict):
        return {k: pretty_format(v, nonestr, emptystr) for k, v in item.items()}
    else:
        if item is None:
            return nonestr
        elif item == '':
            return emptystr
        elif isinstance (item, str):
            return '\'' + item + '\''
        else:
            return item

# def json_ld_to_edn_string(obj):
#    if isinstance(obj, dict):
#        # Check for special types first
#        match obj.get('@type', None):
#            case 'xt:keyword':
#                return edn_format.Keyword(obj['@value'])
#            case 'xt:symbol':
#                return edn_format.Symbol(obj['@value'])
#            case _:
#                # Recursively process and convert nested dictionaries or lists
#                return {k: json_ld_to_edn_string(v) for k, v in obj.items() if k != '@type'}
#    elif isinstance(obj, list):
#        # Recursively process each item in the list
#        return [json_ld_to_edn_string(item) for item in obj]
#    else:
#        # Return the object as is if it's not a dict or list (base case)
#        return obj

class XtdbConsole(cmd.Cmd):
    """
    An XTDB console for processing SQL statements, files, or prompts.
    Running multiple statements (separated by `;`) is not supported.
    Attributes
    ----------
    url : str
        HTTP URL of an XTDB server
    accept : str, optional
        accept header content type (defaults to 'application/json')
    prompt : str, optional
        initial prompt (defaults to '->')
    nested_prompt : str, optional
        prompt for multi-line SQL statements (defaults to '..')
    """
    def __init__(self, url, accept='application/json', prompt='-> ', nested_prompt='.. '):
        super().__init__()
        self.url = url
        self.accept = accept
        self.prompt = prompt
        self.default_prompt = prompt
        self.nested_prompt = nested_prompt
        self.timer = False
        self.txtimeout = "PT1S"
        self.basis_at_tx_id = -1
        self.basis_at_system_time = end_of_time
        self.tabulate = True
        self.lines = []
        self.was_error = False

    def emptyline(self):
        pass

    def do_timer(self, arg):
        'Sets or shows timer.'
        if arg:
            self.timer = arg == 'on'
        if self.timer:
            print('on')
        else:
            print('off')

    def complete_txtimeout(self, text, line, begidx, endidx):
        return [x for x in ['off']
                if x.startswith(text)]

    def do_txtimeout(self, arg):
        'Sets or shows request `txTimeout`.'
        if arg:
            self.txtimeout = arg
        if self.txtimeout:
            print(self.txtimeout)

    def do_basis_at_tx_id(self, arg):
        'Sets or shows current TxId used for the time-travel `basis` at which queries are run.'
        if arg:
            self.basis_at_tx_id = int(arg)
        if self.basis_at_tx_id:
            print(self.basis_at_tx_id)
        # TODO clear_basis?
        # TODO print json toggle (off by default)

    def do_basis_at_system_time(self, arg):
        'Sets or shows current systemTime used for the time-travel `basis` at which queries are run.'
        if arg:
            self.basis_at_system_time = parse_iso_datetime(arg).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
        if self.basis_at_system_time:
            print(self.basis_at_system_time)

    def do_clear_basis(self, arg):
        'Clear current `basis` settings.'
        self.basis_at_system_time = end_of_time
        self.basis_at_tx_id = -1
        print('basis cleared')

    def do_recent_transactions(self, arg):
        'Show the 10 most recent transactions by running `SELECT * FROM xt$txs ORDER BY xt$txs.xt$id DESC LIMIT 20;`'
        result = Xtdb(self.url, self.accept,
                           self.txtimeout).sql_query('SELECT * FROM xt$txs ORDER BY xt$txs.xt$id DESC LIMIT 20;')
        if self.tabulate:
                result2 = [pretty_format(item) for item in result]
                print(markdownSimpleTable(prepare_for_tabulate(result2)))
        else:
            pprint.pprint(result)

    def do_tabulate(self, arg):
        'Sets or shows table printer setting.'
        if arg:
            self.tabulate = arg == 'on'
        if self.tabulate:
            print('on')
        else:
            print('off')

    def do_status(self, arg):
        'Calls the `/status` endpoint of the connected XTDB server.'
        result = Xtdb(self.url, self.accept).sql_status()
        if self.tabulate:
            print(markdownSimpleTable(result))
        else:
            pprint.pprint(result)

    def complete_accept(self, text, line, begidx, endidx):
        return [x for x in ['application/json']
                if x.startswith(text)]

    def do_accept(self, arg):
        'Sets or shows the accepted mime type.'
        if arg:
            self.accept = re.sub('=\s*', '', arg)
        print(self.accept)

    def do_url(self, arg):
        'Sets or shows the database URL.'
        if arg:
            self.url = re.sub('=\s*', '', arg)
        print(self.url)

    def do_quit(self, arg):
        'Quits the console.'
        return 'stop'

    def default(self, line):
        start_time = None
        if line == 'EOF':
            return 'stop'

        self.lines.append(line)

        if not line.strip().endswith(';'):
            self.prompt = self.nested_prompt
            return

        try:
            sql = '\n'.join(self.lines)
            self.lines = []
            self.prompt = self.default_prompt
            start_time = time.time()

            explain = sql.upper().startswith("EXPLAIN ")
            explain = True if explain else None
            sql = sql[8:] if explain else sql

            # Initial conditions for setting values to None or their actual value
            basis_at_tx_id_condition = self.basis_at_tx_id == -1
            basis_at_system_time_condition = self.basis_at_system_time == end_of_time
            # Determine if either value should not be None based on initial conditions
            either_value_not_none = not basis_at_tx_id_condition or not basis_at_system_time_condition
            # Apply logic to ensure both are not None if either is not None
            if either_value_not_none:
                basis_at_tx_id = self.basis_at_tx_id if not basis_at_tx_id_condition else -1
                basis_at_system_time = self.basis_at_system_time if not basis_at_system_time_condition else end_of_time
            else:
                # If both should be None based on their specific conditions
                basis_at_tx_id = None if basis_at_tx_id_condition else self.basis_at_tx_id
                basis_at_system_time = None if basis_at_system_time_condition else self.basis_at_system_time

            # TODO consider WITH ROLLBACK SET BEGIN COMMIT VALUES START

            # TODO figure out insertion of "" and escaped \' edge cases

            dml_operations = ("INSERT", "UPDATE", "DELETE", "ERASE")

            if sql.upper().startswith(dml_operations):
                result = Xtdb(self.url, self.accept, self.txtimeout).sql_tx(sql)
            else:
                result = Xtdb(self.url, self.accept,
                                   self.txtimeout).sql_query(sql,
                                                             explain=explain,
                                                             basis_at_tx_id=basis_at_tx_id,
                                                             basis_at_system_time=basis_at_system_time)

            if explain:
                #print('Generated query plan (printed using `edn_format`):')
                print(result[0]['plan'])
                #print(edn_format.dumps(json_ld_to_edn_string(result[0]['plan']),
                #                       keyword_keys=True,
                #                       indent=2))
            elif self.tabulate:
                result2 = [pretty_format(item) for item in result]
                print(markdownSimpleTable(prepare_for_tabulate(result2)))
            else:
                pprint.pprint(result)

        except urllib.error.HTTPError as e:
            self.was_error = True
            if e.code != 400:
                print('%s %s' % (e.code, e.reason))
            body = e.read().decode('utf-8')
            if body:
               # print(body)
               # TODO if no xtdb.error/message, print the whole body
                print(json.loads(body)["@value"]["xtdb.error/message"])
        except urllib.error.URLError as e:
            self.was_error = True
            print(self.url)
            print(e.reason)
        if start_time and self.timer:
            print('Elapsed: %f ms' % (time.time() - start_time))

def main():
    import argparse
    import sys
    import pathlib

    print("XTDB v2.0 -- SQL Console")
    print("Copyright © 2021-2024, JUXT LTD.")
    print()
    print("Type 'help' for commands")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sql', nargs='*', help='SQL statement or file')
    parser.add_argument('--url', default='http://localhost:3000')
    args = parser.parse_args()

    prompt = '-> '
    nested_prompt = '.. '
    if not sys.stdin.isatty():
        prompt = ''
        nested_prompt = ''
    try:
        console = XtdbConsole(args.url, prompt=prompt, nested_prompt=nested_prompt)
        if args.sql:
            for sql in args.sql:
                path = pathlib.Path(sql)
                if path.is_file():
                    sql = path.read_text()

                if not sql.strip().endswith(';'):
                    sql += ';'

                console.default(sql)

                if console.was_error:
                    sys.exit(1)
        else:
            console.cmdloop()
            if sys.stdin.isatty():
                print()
    except KeyboardInterrupt:
        print()

if __name__ == '__main__':
    main()
