import re
import datetime
import dateutil.parser


class CSVParserGenerator:
    """Memory-efficient CSV parser using generators for line-by-line processing."""

    def __init__(self):
        self.line_regex = re.compile(r""""((?:[^"]|"")*)"|[^,"\n\r]*(?:,|\r?\n|\r|$)""")
        self.data = {}

    def _stream_file_lines(self, filename):
        """Generator that streams lines from the file one at a time."""
        try:
            with open(filename, 'r') as f:
                for line in f:
                    yield line
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{filename}' not found.")
        except IOError as e:
            raise IOError(f"Error reading file '{filename}': {e}")

    def read_csv(self, filename):
        """Main method to read and parse the CSV file."""
        # Initialize state for this parse
        self._header = None
        self._rows = None
        self.data = {}

        try:
            self._extract_rows(filename)
            self._build_column_dict()
            return self.data
        except Exception as e:
            # Return partial results with error messages
            print(f"Warning: Error during parsing: {e}")
            if self.data:
                print("Returning partial results...")
            return self.data

    def _extract_rows(self, filename):
        """Extract all rows handling multi-line quoted fields (RFC 4180)."""
        # Read entire file content to handle multi-line quoted fields
        try:
            with open(filename, 'r', encoding='utf-8-sig') as f:  # utf-8-sig removes BOM
                content = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{filename}' not found.")
        except IOError as e:
            raise IOError(f"Error reading file '{filename}': {e}")

        # Parse the CSV content with multi-line support
        all_lines = self._parse_csv_content(content)

        if not all_lines:
            print("Warning: No lines found in file.")
            self._header = []
            self._rows = []
            return self._rows

        # Detect if first line is a header
        has_header = self._detect_header(all_lines)

        if has_header:
            # First line is the header
            self._header = [header.replace('"', '') for header in all_lines[0]]
            self._rows = all_lines[1:]
        else:
            # No header - generate column names
            self._rows = all_lines
            num_columns = len(all_lines[0]) if all_lines else 0
            self._header = [f"column_{idx}" for idx in range(1, num_columns + 1)]

        if not self._rows:
            print("Warning: No data rows found in file.")
            return self._rows

        # Padding cleanup - ensure all rows have same length
        longest_row = max((len(row) for row in self._rows)) if self._rows else 0

        # Warn about inconsistent column counts
        expected_cols = len(self._header) if self._header else longest_row
        inconsistent_rows = []
        for idx, row in enumerate(self._rows, start=2 if has_header else 1):
            if len(row) != expected_cols and len(row) != longest_row:
                inconsistent_rows.append((idx, len(row)))

        if inconsistent_rows:
            print(f"Warning: Detected {len(inconsistent_rows)} row(s) with inconsistent column count")
            print(f"  Expected {expected_cols} columns, but found:")
            for row_num, col_count in inconsistent_rows[:5]:
                print(f"    Line {row_num}: {col_count} columns")
            if len(inconsistent_rows) > 5:
                print(f"    ... and {len(inconsistent_rows) - 5} more")
            print(f"  Missing columns filled with empty values.")

        self._rows = [row + [''] * (longest_row - len(row)) for row in self._rows]

        # Ensure header has enough columns
        if self._header:
            header_len = len(self._header)
            if longest_row > header_len:
                self._header = self._header + [f"column_{idx}" for idx in range(header_len + 1, longest_row + 1)]
        else:
            self._header = [f"column_{idx}" for idx in range(1, longest_row + 1)]

        return self._rows

    def _parse_csv_content(self, content):
        """
        Parse CSV content with support for multi-line quoted fields (RFC 4180).

        Returns:
            list: List of rows, where each row is a list of field values
        """
        rows = []
        current_row = []
        current_field = []
        in_quotes = False
        i = 0
        line_num = 1

        while i < len(content):
            char = content[i]

            if in_quotes:
                # We're inside a quoted field
                if char == '"':
                    # Check if it's an escaped quote ("") or end of field
                    if i + 1 < len(content) and content[i + 1] == '"':
                        # Escaped quote - add single quote to field
                        current_field.append('"')
                        i += 2  # Skip both quotes
                        continue
                    else:
                        # End of quoted field
                        in_quotes = False
                        i += 1

                        # After closing quote, we expect comma, newline, or end of file
                        # Skip any whitespace
                        while i < len(content) and content[i] in (' ', '\t'):
                            i += 1

                        if i < len(content) and content[i] not in (',', '\n', '\r'):
                            # Error: unexpected character after closing quote
                            print(f"Warning: Line {line_num}: Unexpected character '{content[i]}' after closing quote. Attempting recovery...")
                            # Try to recover by skipping to next comma or newline
                            while i < len(content) and content[i] not in (',', '\n', '\r'):
                                i += 1
                        continue
                else:
                    # Regular character inside quoted field (including newlines)
                    current_field.append(char)
                    if char == '\n':
                        line_num += 1
                    i += 1
            else:
                # We're outside quotes
                if char == '"':
                    # Start of quoted field
                    in_quotes = True
                    i += 1
                elif char == ',':
                    # End of field
                    current_row.append(''.join(current_field))
                    current_field = []
                    i += 1
                elif char == '\n' or (char == '\r' and i + 1 < len(content) and content[i + 1] == '\n'):
                    # End of row
                    current_row.append(''.join(current_field))
                    current_field = []

                    if current_row:  # Don't add empty rows
                        rows.append(current_row)
                    current_row = []

                    # Skip \r\n together
                    if char == '\r':
                        i += 2
                    else:
                        i += 1
                    line_num += 1
                elif char == '\r':
                    # Handle standalone \r as line ending
                    current_row.append(''.join(current_field))
                    current_field = []

                    if current_row:
                        rows.append(current_row)
                    current_row = []
                    i += 1
                    line_num += 1
                else:
                    # Regular character
                    current_field.append(char)
                    i += 1

        # Handle end of file
        if current_field or current_row:
            current_row.append(''.join(current_field))
        if current_row:
            rows.append(current_row)

        # Check for unclosed quotes
        if in_quotes:
            print(f"Warning: Unclosed quoted field at end of file. Attempting recovery...")

        return rows

    def _detect_header(self, all_lines):
        """
        Detect if the first line is a header or data.

        Strategy:
        - If we only have one line, assume it's a header
        - Check if first row values look like numeric/date data
        - If first row is all non-numeric strings and second row has numbers/dates, it's likely a header
        """
        if not all_lines:
            return False

        if len(all_lines) == 1:
            # Only one line - assume it's a header
            return True

        first_row = all_lines[0]

        # Count how many values in the first row look like numbers or dates
        numeric_count = 0
        for value in first_row:
            if value == '':
                continue

            # Try to parse as number
            try:
                float(value)
                numeric_count += 1
                continue
            except ValueError:
                pass

            # Try to parse as date
            try:
                dateutil.parser.parse(value)
                numeric_count += 1
            except (ValueError, TypeError):
                pass

        # If more than half of the first row values are numeric/dates,
        # it's likely data, not a header
        non_empty = [v for v in first_row if v != '']
        if non_empty and numeric_count > len(non_empty) / 2:
            return False

        # Otherwise, assume it's a header
        return True

    def _build_column_dict(self):
        """Build dictionary of columns with type detection and conversion."""
        if not self._header or not self._rows:
            return

        for col_index in range(len(self._header)):
            column = []
            for row in self._rows:
                try:
                    column.append(row[col_index])
                except IndexError:
                    pass

            try:
                self.data[self._header[col_index]] = self._convert_column_types(column, self._header[col_index])
            except Exception as e:
                print(f"Warning: Error processing column '{self._header[col_index]}': {e}")
                # Store as strings if type detection fails
                self.data[self._header[col_index]] = column

    def _convert_column_types(self, column, column_name):
        """Convert column values to detected data type."""
        data_type = self._infer_data_type(column)

        # Check for date ambiguity if column contains dates
        if data_type == datetime.datetime:
            self._check_date_ambiguity(column, column_name)

        try:
            if data_type in {str, float, int}:
                return [data_type(value) if value != '' else value for value in column]
            elif data_type == datetime.datetime:
                return [dateutil.parser.parse(value) if value != '' else value for value in column]
            else:
                raise TypeError(f'Unsupported value type of {data_type}')
        except Exception as e:
            # If conversion fails, return as strings
            print(f"Warning: Type conversion failed, returning as strings: {e}")
            return column

    def _check_date_ambiguity(self, column, column_name):
        """
        Check for ambiguous date formats in a column and print warnings.

        Detects:
        - Dates that could be interpreted multiple ways (e.g., 01/02/03)
        - Mixed date formats in the same column
        """
        date_formats_found = set()
        ambiguous_dates = []

        for idx, value in enumerate(column):
            if value == '':
                continue

            # Check if this value could be an ambiguous date
            is_ambiguous = False

            # Pattern: numeric values that look like dates
            # e.g., "20200101" could be a date or a number
            if value.isdigit() and len(value) == 8:
                ambiguous_dates.append(f"Row {idx+1}: '{value}' (could be integer or date YYYYMMDD)")
                continue

            # Check for dates with slashes or dashes
            if '/' in value or '-' in value:
                parts = value.replace('/', '-').split('-')[0:3]  # Get date parts only
                if len(parts) >= 2:
                    try:
                        # Parse all numeric parts
                        nums = []
                        for part in parts:
                            # Extract just the numeric part (ignore time)
                            num_part = part.split(' ')[0]
                            if num_part.isdigit():
                                nums.append(int(num_part))

                        if len(nums) >= 2:
                            # If day and month are both <= 12, it's ambiguous
                            if len(nums) >= 2 and all(n <= 12 for n in nums[:2]):
                                is_ambiguous = True
                                ambiguous_dates.append(f"Row {idx+1}: '{value}' (dd/mm vs mm/dd ambiguity)")

                            # Detect format
                            if len(nums) == 3:
                                # Check if it looks like YYYY-MM-DD vs DD-MM-YYYY
                                if nums[0] > 31:  # Likely YYYY-MM-DD
                                    date_formats_found.add('YYYY-MM-DD')
                                elif nums[2] > 31:  # Likely DD-MM-YYYY or MM-DD-YYYY
                                    date_formats_found.add('DD-MM-YYYY or MM-DD-YYYY')
                                else:
                                    date_formats_found.add('ambiguous')
                    except:
                        pass

        # Print warnings
        if len(date_formats_found) > 1:
            print(f"Warning: Column '{column_name}' contains dates with different formats: {date_formats_found}")
            print(f"  This may lead to incorrect date parsing.")

        if ambiguous_dates and len(ambiguous_dates) <= 5:
            print(f"Warning: Column '{column_name}' contains ambiguous date values:")
            for amb in ambiguous_dates[:5]:
                print(f"  {amb}")
            if len(ambiguous_dates) > 5:
                print(f"  ... and {len(ambiguous_dates) - 5} more")
        elif len(ambiguous_dates) > 5:
            print(f"Warning: Column '{column_name}' contains {len(ambiguous_dates)} ambiguous date values")
            print(f"  Examples: {ambiguous_dates[0]}, {ambiguous_dates[1]}")

    def _infer_data_type(self, column):
        """Infer the predominant data type in a column by testing conversions."""
        if not column:
            return str

        types = set()
        for value in column:
            if value == '':  # Skip empty values
                continue

            # Try int first
            try:
                int(value)
                types.add(int)
                continue
            except ValueError:
                pass

            # Try float
            try:
                float(value)
                types.add(float)
                continue
            except ValueError:
                pass

            # Try datetime
            try:
                dateutil.parser.parse(value)
                types.add(datetime.datetime)
            except (ValueError, TypeError):
                types.add(str)

        # Type precedence: str > float > int > datetime
        if str in types:
            return str
        elif float in types:
            return float
        elif int in types:
            return int
        elif datetime.datetime in types:
            return datetime.datetime
        return str


def display_data_structure(data_structure):
    """
    Display a parsed CSV data structure with type information.

    Args:
        data_structure (dict): Dictionary returned by csv_parser()
    """
    if not data_structure:
        print("No data to display")
        return

    # Show structure
    print(f"Columns: {list(data_structure.keys())}")
    print(f"\nNumber of rows: {len(next(iter(data_structure.values())))}")

    # Show data types and first few values
    for col_name, values in data_structure.items():
        sample_value = values[0] if values else None
        print(f"\n{col_name}:")
        print(f"  Type: {type(sample_value).__name__}")
        print(f"  First 10 values: {values[:10]}")


def csv_parser(filename):
    """
    Main CSV parser function for use in Jupyter notebook.

    This parser uses a generator-based approach for memory-efficient line-by-line processing.
    It automatically detects and converts data types (int, float, str, datetime).

    Args:
        filename (str): Path to the CSV file to parse

    Returns:
        dict: Dictionary with column names as keys and lists of typed values
    """
    return CSVParserGenerator().read_csv(filename)
