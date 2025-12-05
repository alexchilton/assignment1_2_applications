import math
from csv_parser_generator import csv_parser


class StatisticsCalculator:
    """Simple statistics calculator for CSV data."""

    def __init__(self):
        """Initialize the statistics calculator."""
        self.stats = {}

    def compute_statistics(self, filename):
        """
        Compute min, max, mean, and standard deviation for numeric columns in a CSV file.

        Args:
            filename (str): Path to the CSV file

        Returns:
            dict: Dictionary with column names as keys and statistics as values
                  Format: {
                      'column_name': {
                          'min': value,
                          'max': value,
                          'mean': value,
                          'std_dev': value
                      },
                      ...
                  }
        """
        # Use Task 1 parser to read the data
        data = csv_parser(filename)

        # Reset statistics
        self.stats = {}

        # Compute statistics for each column
        for column_name, values in data.items():
            # Filter out empty values and check if column is numeric
            numeric_values = self._get_numeric_values(values)

            if numeric_values:
                # Compute statistics for numeric columns
                self.stats[column_name] = {
                    'min': min(numeric_values),
                    'max': max(numeric_values),
                    'mean': self._calculate_mean(numeric_values),
                    'std_dev': self._calculate_std_dev(numeric_values)
                }
            else:
                # Skip non-numeric columns (e.g., DateTime, strings)
                pass

        return self.stats

    def _get_numeric_values(self, values):
        """
        Extract numeric values from a list, filtering out empty strings and non-numeric types.

        Args:
            values (list): List of values from a column

        Returns:
            list: List of numeric values (int or float)
        """
        numeric_values = []
        for value in values:
            # Skip empty strings
            if value == '':
                continue

            # Check if value is numeric (int or float)
            if isinstance(value, (int, float)):
                numeric_values.append(value)

        return numeric_values

    def _calculate_mean(self, values):
        """
        Calculate the mean (average) of a list of numbers.

        Args:
            values (list): List of numeric values

        Returns:
            float: Mean value
        """
        if not values:
            return 0
        return sum(values) / len(values)

    def _calculate_std_dev(self, values):
        """
        Calculate the standard deviation of a list of numbers.

        Args:
            values (list): List of numeric values

        Returns:
            float: Standard deviation
        """
        if not values or len(values) < 2:
            return 0

        mean = self._calculate_mean(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)


def compute_statistics(filename):
    """
    Convenience function to compute statistics for a CSV file.

    This function provides the simple interface required by the notebook template:
    data_structure_statistics = compute_statistics('data_file.csv')

    Args:
        filename (str): Path to the CSV file

    Returns:
        dict: Dictionary with column names as keys and statistics as values
    """
    calculator = StatisticsCalculator()
    return calculator.compute_statistics(filename)
