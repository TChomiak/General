# Import Libraries
import os                # File and Data Manipulation
import numpy as np       # Numerical Operations
import pandas as pd      # Data Manipultions
#os.chdir('E:\\Navarro')  # Define Working Directory
#os.getcwd()              # Get current working directory
import matplotlib.pyplot as plt       # creating static, interactive, and animated visualizations
plt.style.use("default")              # Set the default style matplotlib plots
import matplotlib.colors as mcolors   # colors for legend
import matplotlib.patches as mpatches # drawing patches (shapes) for legend
import matplotlib.ticker as mticker
from matplotlib.ticker import FormatStrFormatter, MaxNLocator, MultipleLocator, AutoMinorLocator
import matplotlib.cm as cm
from matplotlib.colors import Normalize
import seaborn as sns # Data Visualization
from collections import Counter
from itertools import chain
import re
import warnings
#warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings('ignore')

# Function for Unit Conversion
def unit_conversion_mass(value, from_unit, to_unit):
    """
    Convert between weight units: pounds (lbs), kilograms (kg), and tons.

    Parameters:
    - value: float, the numerical value to convert
    - from_unit: str, the unit of the input value ('lbs', 'kg', 'tons')
    - to_unit: str, the desired output unit ('lbs', 'kg', 'tons')

    Returns:
    - float, the converted value in the desired unit
    """
    # Conversion factors
    lbs_to_kg = 0.45359237
    kg_to_lbs = 1 / lbs_to_kg
    tons_to_kg = 907.18474
    kg_to_tons = 1 / tons_to_kg

    # Conversion logic
    if from_unit == "lbs" and to_unit == "kg":
        return value * lbs_to_kg
    elif from_unit == "kg" and to_unit == "lbs":
        return value * kg_to_lbs
    elif from_unit == "tons" and to_unit == "kg":
        return value * tons_to_kg
    elif from_unit == "kg" and to_unit == "tons":
        return value * kg_to_tons
    elif from_unit == "lbs" and to_unit == "tons":
        return (value * lbs_to_kg) * kg_to_tons
    elif from_unit == "tons" and to_unit == "lbs":
        return (value * tons_to_kg) * kg_to_lbs
    elif from_unit == to_unit:
        return value  # No conversion needed
    else:
        raise ValueError("Invalid units. Supported units: 'lbs', 'kg', 'tons'.")

# Function for Unit Conversion
def unit_conversion(value, from_unit, to_unit):
    """
    Convert between mass units (lbs, kg, tons) and volume units (gal, m3, cm3, ft3, yd3, in3).

    Parameters:
        value (float): The numerical value to convert.
        from_unit (str): The unit of the input value (e.g., 'lbs', 'kg', 'tons', 'gal', 'm3', 'cm3', 'ft3', 'yd3', 'in3').
        to_unit (str): The desired output unit (e.g., 'lbs', 'kg', 'tons', 'gal', 'm3', 'cm3', 'ft3', 'yd3', 'in3').

    Returns:
        float: The converted value in the desired unit.
    """
    # Comprehensive conversion factors
    conversion_factors = {
        # Mass Conversions
        ("lbs", "kg"): 0.45359237,
        ("kg", "tons"): 1 / 907.18474,
        ("tons", "lbs"): 2000,
        # Volume Conversions
        ("gal", "m3"): 0.00378541,
        ("gal", "cm3"): 3785.41,
        ("gal", "ft3"): 1 / 7.48052,
        ("gal", "in3"): 231,
        ("gal", "yd3"): 0.00378541 / 0.764554857984,
        ("m3", "cm3"): 1000000,
        ("m3", "in3"): 1 / 0.000016387,
        ("m3", "ft3"): 35.3147,
        ("m3", "yd3"): 1 / 0.764554857984,
        ("ft3", "cm3"): 0.0283168 * 1000000,
        ("ft3", "in3"): 1728,
        ("ft3", "yd3"): 1 / 27,
        ("yd3", "cm3"): 0.764554857984 * 1000000,
        ("yd3", "in3"): 27 * 1728,
        ("cm3", "in3"): 1 / 16.387064,
    }

    # Automatically generate reverse conversions
    for (from_unit_key, to_unit_key), factor in list(conversion_factors.items()):
        conversion_factors[(to_unit_key, from_unit_key)] = 1 / factor

    # Direct conversion if available
    if (from_unit, to_unit) in conversion_factors:
        return value * conversion_factors[(from_unit, to_unit)]

    # Unsupported conversion
    raise ValueError(
        f"Invalid conversion from '{from_unit}' to '{to_unit}'. Supported units: "
        f"{', '.join(set(u for pair in conversion_factors for u in pair))}."
    )

# Load and Clean Data
def load_and_clean_data(file_path, sheet_name, na_values=['', 'NA']):
    """
    Load the data from an Excel file.
    Clean the data (Remove Spaces).
    Process the Date column (if it exists) to add Month and Month_Name columns.
    Convert inf values to NaN.
    Rename columns to replace spaces with underscores.

    Parameters:
    - file_path: str, the path to the Excel file.
    - sheet_name: str, the name of the sheet to load.

    Returns:
    - df: pd.DataFrame, the cleaned DataFrame.
    """
    # Load the data
    df = pd.read_excel(file_path, sheet_name=sheet_name, na_values=na_values, keep_default_na=False)
    
    # Rename columns to replace spaces with underscores
    df.columns = df.columns.str.replace(' ', '_')
    
    # Clean string columns by removing extra spaces
    df = df.applymap(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
    
    # Replace infinite values with NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Check if the 'Date' column exists
    if 'Date' in df.columns:
        # Ensure the Date column is in datetime format
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Add a reformatted Date column
        df.loc[:, 'date'] = df['Date'].dt.strftime('%Y-%b-%d')
        
        # Add new columns for Month and Month Name
        df['Month'] = df['Date'].dt.month
        df['Month_Name'] = df['Date'].dt.strftime('%B')
    
    return df

# Filter Dataframe Based on Keywords
def filter_by_keywords(df, keywords, search_column, output_name):
    """
    Filter a DataFrame based on keywords in a specific column and handle potential errors.

    Parameters:
    - df: pd.DataFrame, the input DataFrame to filter.
    - keywords: list of str, keywords to search for in the specified column.
    - search_column: str, the name of the column to search in.
    - output_name: str, the desired name for the output DataFrame.

    Returns:
    - pd.DataFrame: A filtered DataFrame containing rows that match the keywords.
    """
    # Select a Column
    try:
        # Check if the column exists
        if search_column not in df.columns:
            raise KeyError(f"Column '{search_column}' does not exist in the DataFrame.")
        
        # Replace NaN values with an empty string to prevent errors in str.contains
        df[search_column] = df[search_column].fillna('')

        # Escape any special regex characters in the keywords to avoid unintended behavior
        import re
        pattern = '|'.join(re.escape(term) for term in keywords)

        # Filter rows where the specified column contains any of the keywords
        filtered_df = df[df[search_column].str.contains(pattern, case=False, regex=True)]

        # Check if the filtered DataFrame is empty
        if filtered_df.empty:
            print(f"No matching rows found for keywords {keywords} in column '{search_column}'.")
        else:
            # Get unique counts and entries (optional debugging step)
            n_unique_entries = filtered_df[search_column].nunique()
            print(f"Unique matching entries in '{search_column}': {n_unique_entries}")
            print(f"Sample unique entries: {filtered_df[search_column].unique()[:5]}")

        # Return the filtered DataFrame
        print(f"DataFrame '{output_name}' created successfully.")
        return filtered_df

    except KeyError as e:
        print(f"KeyError: {e}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Sort Unique Numbers
def get_unique_sorted_numbers(df, column_name):
    """
    Get sorted unique numeric entries from a specified column.

    Parameters:
    - df: pd.DataFrame, the DataFrame to process
    - column_name: str, the name of the column to get unique numbers from

    Returns:
    - sorted_unique_numbers: list, sorted unique numeric entries from the column
    """
    try:
        # Check if the column exists
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")

        # Convert column to numeric, coercing non-numeric values to NaN
        numeric_column = pd.to_numeric(df[column_name], errors='coerce')

        # Get unique numbers, drop NaN values, and sort
        unique_numbers = numeric_column.dropna().unique()
        sorted_unique_numbers = sorted(unique_numbers)

        return sorted_unique_numbers

    except KeyError as e:
        print(f"KeyError: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

# Find and Count Unique Entries of a Column
def find_unique_numbers_and_counts_0(df, column_name):
    """
    Finds all unique numeric entries in a column without splitting based on any delimiter,
    counts the occurrences of each number, and sorts them by count.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        pd.DataFrame: A DataFrame containing unique numbers and their counts, sorted by count.
    """
    try:
        # Step 1: Check if the column exists
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")
        
        # Step 2: Convert column to numeric, coercing non-numeric values to NaN
        numeric_column = pd.to_numeric(df[column_name], errors='coerce')
        
        # Step 3: Drop NaN values
        numeric_values = numeric_column.dropna()
        
        # Step 4: Count occurrences of unique numbers
        counts = Counter(numeric_values)
        
        # Step 5: Sort counts by frequency in descending order
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        
        # Step 6: Convert to DataFrame
        result_df = pd.DataFrame(sorted_counts, columns=['Entry', 'Count'])
        
        return result_df

    except KeyError as e:
        print(f"KeyError: {e}")
        return pd.DataFrame(columns=['Number', 'Count'])  # Return empty DataFrame on error
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame(columns=['Number', 'Count'])  # Return empty DataFrame on error

# Find and Count Unique Entries of a Column with Multiple Entries
def find_unique_numbers_and_counts(df, column_name):
    """
    Finds all unique numeric entries in a column, counts their occurrences, 
    and returns a DataFrame sorted by the count.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        pd.DataFrame: A DataFrame containing unique numbers and their counts, sorted by count.
    """
    try:
        # Check if the column exists
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")
        
        # Convert column to numeric, coercing non-numeric values to NaN
        numeric_column = pd.to_numeric(df[column_name], errors='coerce')
        
        # Drop NaN values
        numeric_values = numeric_column.dropna()
        
        # Count occurrences of each unique number
        counts = Counter(numeric_values)
        
        # Sort counts by frequency in descending order
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        
        # Convert to DataFrame
        result_df = pd.DataFrame(sorted_counts, columns=['Entry', 'Count'])
        
        return result_df

    except KeyError as e:
        print(f"KeyError: {e}")
        return pd.DataFrame(columns=['Number', 'Count'])  # Return empty DataFrame on error
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame(columns=['Number', 'Count'])  # Return empty DataFrame on error

# Find and Count Unique Entries of a Column
def find_unique_text_and_counts_0(df, column_name):
    """
    Get sorted unique string entries from a specified column.
    
    Parameters:
    - df: pd.DataFrame, the DataFrame to process
    - column_name: str, the name of the column to get unique strings from
    
    Returns:
    - sorted_unique_strings: list, sorted unique string entries from the column
    """
    unique_strings = [x for x in df[column_name].unique() if isinstance(x, str)]
    sorted_unique_strings = sorted(unique_strings, reverse=False)
    return sorted_unique_strings
    
        
# Find and Count Unique Entries of a Column
def find_unique_text_and_counts_1(df, column_name):
    """
    Finds all unique entries in a column without splitting based on any delimiter,
    counts the occurrences of each entry, and sorts them by count.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        pd.DataFrame: A DataFrame containing unique entries and their counts, sorted by count.
    """
    # Step 1: Replace NaN with empty strings and convert to string
    temp_column = df[column_name].fillna('').astype(str)

    # Step 2: Strip whitespace and filter out empty entries
    cleaned_entries = temp_column.str.strip()  # Strip leading/trailing whitespace
    cleaned_entries = cleaned_entries[cleaned_entries != '']  # Remove empty strings

    # Step 3: Count occurrences of unique entries
    counts = Counter(cleaned_entries)

    # Step 4: Sort and convert to DataFrame
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    result_df = pd.DataFrame(sorted_counts, columns=['Entry', 'Count'])
    
    return result_df

# Find and Count Unique Entries of a Column with Multiple Entries
def find_unique_text_and_counts_2(df, column_name, delimiter=','):
    """
    Finds all unique entries in a column where cells may contain multiple, delimiter-separated values,
    counts the occurrences of each entry, and sorts them by count.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.
        delimiter (str): The delimiter separating multiple entries in a cell. Default is ','.

    Returns:
        pd.DataFrame: A DataFrame containing unique entries and their counts, sorted by count.
    """
    # Step 1: Replace NaN with empty strings and convert to string
    temp_column = df[column_name].fillna('').astype(str)
    
    # Step 2: Split, strip whitespace, and filter out empty entries
    split_values = temp_column.str.split(delimiter).apply(
        lambda x: [item.strip() for item in x if item.strip()]  # Remove empty/whitespace-only entries
    )
    
    # Step 3: Flatten the list of lists
    all_entries = chain.from_iterable(split_values)
    
    # Step 4: Count occurrences
    counts = Counter(all_entries)
    
    # Step 5: Sort and convert to DataFrame
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    result_df = pd.DataFrame(sorted_counts, columns=['Entry', 'Count'])
    
    return result_df

# Optimized
def find_unique_text_and_counts_optimized(df, column_name, delimiter=',', case_sensitive=True):
    """
    Optimized version to find unique entries and counts in a column with multiple entries.
    Handles large datasets more efficiently using pandas' vectorized operations.
    """
    # Replace NaN with empty strings and split the column
    temp_column = df[column_name].fillna('').astype(str).str.split(delimiter)

    # Explode into rows, strip whitespace, and handle case sensitivity
    exploded = temp_column.explode().str.strip()
    if not case_sensitive:
        exploded = exploded.str.lower()

    # Filter out empty entries
    valid_entries = exploded[exploded != '']

    # Count occurrences and sort
    counts = valid_entries.value_counts().reset_index()
    counts.columns = ['Entry', 'Count']

    return counts

# Plot Results of find_unique_entries_and_counts
def plot_horizontal_bar(df, title='Count of Unique Entries', y_label='', figsize=(9, 6), save_folder=None, file_name=None):
    """
    Plots a horizontal bar chart of unique entries and their counts with improved aesthetics.
    Optionally saves the plot to a file in the specified folder.

    Parameters:
        df (pd.DataFrame): A DataFrame containing 'Entry' and 'Count' columns.
        title (str): The title of the plot.
        y_label (str): The title for the y-axis.
        figsize (tuple): A tuple specifying the figure size (width, height). Default is (9, 6).
        save_folder (str, optional): The folder where the plot will be saved.
        file_name (str, optional): The name of the file, including its extension (e.g., 'plot.png').
    """
    # Ensure input DataFrame has the required columns
    if 'Entry' not in df.columns or 'Count' not in df.columns:
        raise ValueError("Input DataFrame must contain 'Entry' and 'Count' columns.")

    # Check if 'Entry' column is a string type, convert to string if not
    if not pd.api.types.is_string_dtype(df['Entry']):
        df['Entry'] = df['Entry'].astype(str)

    # Set up the figure with customizable size
    plt.figure(figsize=figsize)

    # Create the horizontal bar plot
    sns.barplot(
        x='Count',
        y='Entry',
        data=df,
        palette='Blues_r',
        edgecolor='black'
    )

    # Customize the plot
    plt.title(title, fontsize=18, pad=10)
    plt.xlabel('Count', fontsize=16, labelpad=15)
    plt.ylabel(y_label, fontsize=16, labelpad=15)
    plt.grid(axis='x', linestyle='--', linewidth=0.5)

    # Add count labels to the bars
    for index, value in enumerate(df['Count']):
        plt.text(value + 0.5, index, str(value), va='center', ha='left', fontsize=10, color='black')

    # Ensure layout is clean and unclipped
    plt.tight_layout()

    # Save the plot if save_folder and file_name are specified
    if save_folder and file_name:
        os.makedirs(save_folder, exist_ok=True)  # Create the folder if it doesn't exist
        file_path = os.path.join(save_folder, file_name)
        plt.savefig(file_path, dpi=600, bbox_inches='tight')  # High-resolution save
        print(f"Plot saved to: {file_path}")

    # Show the plot
    plt.show()


# Map WIWPS_ID to WIWPS_Name for Cells that may contain multiple entries
def map_multiple_entries(df, column_to_map, mapping_dict, output_column, delimiter_pattern=r"[,\s]*'|[,\s]+", quote_format=True):
    """
    Maps values in a column based on a mapping dictionary, handling cells with multiple entries separated
    by complex delimiters, and optionally populates a new column with mapped results in '','','' format.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_to_map (str): The column containing values to be mapped.
        mapping_dict (dict): A dictionary to map values.
        output_column (str): The name of the new column to populate with mapped values.
        delimiter_pattern (str): Regex pattern to split multiple entries. Default handles commas and spaces.
        quote_format (bool): If True, wraps mapped results in '','','' format. If False, joins with commas without quotes.

    Returns:
        pd.DataFrame: The DataFrame with the new column populated.
    """
    def map_cell(cell):
        # Handle NaN or empty cells by returning "NA"
        if pd.isna(cell):
            return "NA"

        # Split the cell into individual entries using the regex pattern
        entries = re.split(delimiter_pattern, str(cell))

        # Strip whitespace and remove empty entries
        entries = [entry.strip().strip("'") for entry in entries if entry.strip()]

        # Map each entry using the mapping dictionary; replace unmatched entries with "NA"
        mapped_entries = [mapping_dict.get(entry, "NA") for entry in entries]

        # Format the output based on quote_format
        if quote_format:
            return "'" + "','".join(mapped_entries) + "'"  # Add quotes around each entry
        else:
            return ", ".join(mapped_entries)  # Plain comma-separated values

    # Apply the mapping function to the column
    df[output_column] = df[column_to_map].apply(map_cell)
    return df

# Map WIWPS_ID to Shipping Information for Cells that may contain multiple entries
def map_to_multiple_columns(df, column_to_map, mapping_dict, prefix, delimiter_pattern=r"[,\s]*'|[,\s]+"):
    """
    Maps values in a column to multiple new columns based on a mapping dictionary.
    Each new column corresponds to one mapped value in the row.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        column_to_map (str): The column containing values to be mapped.
        mapping_dict (dict): A dictionary to map values.
        prefix (str): Prefix for the new columns. Columns will be named as {prefix}_1, {prefix}_2, etc.
        delimiter_pattern (str): Regex pattern to split multiple entries. Default handles commas and spaces.

    Returns:
        pd.DataFrame: The DataFrame with new columns added.
    """
    def map_and_split(cell):
        # Handle NaN or empty cells
        if pd.isna(cell):
            return []

        # Split the cell into individual entries using the regex pattern
        entries = re.split(delimiter_pattern, str(cell))

        # Strip whitespace and remove empty entries
        entries = [entry.strip().strip("'") for entry in entries if entry.strip()]

        # Map each entry using the mapping dictionary; replace unmatched entries with "NA"
        mapped_entries = [mapping_dict.get(entry, "NA") for entry in entries]
        return mapped_entries

    # Apply the mapping and generate a list of lists
    mapped_values = df[column_to_map].apply(map_and_split)

    # Determine the maximum number of new columns needed
    max_columns = mapped_values.map(len).max()

    # Add new columns to the DataFrame
    for i in range(max_columns):
        df[f"{prefix}_{i+1}"] = mapped_values.apply(lambda x: x[i] if i < len(x) else "NA")

    return df

# Mapping Process:
# Splits the values in the specified column using the provided delimiter pattern. 
# Maps each value using the dictionary, replacing unmatched entries with "NA". 

# Dynamic Column Addition:
# The number of new columns is determined by the maximum number of mapped entries in any cell. 
# New columns are named with the provided prefix, suffixed with _1, _2, etc.

# Padding with "NA":
# Rows with fewer mapped entries than the maximum are padded with "NA" to ensure 
# consistent column alignment.

# Map WIWPS_ID to Shipping Information for Cells that may contain multiple entries
def map_and_add_columns(df_1, df_2, column_to_map, mapping_dict, columns_to_add, delimiter_pattern=r"[,\s]*'|[,\s]+"):
    """
    Maps values in df_2 to rows in df_1 based on a mapping dictionary, then pulls specific columns
    from df_1 and adds them to df_2.

    Parameters:
        df_1 (pd.DataFrame): The source DataFrame containing rows to pull columns from.
        df_2 (pd.DataFrame): The target DataFrame where new columns will be added.
        column_to_map (str): Column in df_2 to use for mapping.
        mapping_dict (dict): A dictionary for mapping values in df_2 to a corresponding column in df_1.
        columns_to_add (list): List of column names from df_1 to add to df_2.
        delimiter_pattern (str): Regex pattern to split multiple entries in df_2's mapping column.

    Returns:
        pd.DataFrame: The updated df_2 with the specified columns from df_1 added.
    """
    def map_row(cell):
        """
        Maps a cell's values to a key in the mapping dictionary, finds the matching rows in df_1, 
        and retrieves the requested columns.
        """
        # Handle NaN or empty cells
        if pd.isna(cell):
            return pd.DataFrame([["NA"] * len(columns_to_add)], columns=columns_to_add)

        # Split the cell into entries using the regex pattern
        entries = re.split(delimiter_pattern, str(cell))
        entries = [entry.strip().strip("'") for entry in entries if entry.strip()]

        # Map each entry and find the matching rows in df_1
        mapped_keys = [mapping_dict.get(entry, None) for entry in entries if entry]
        matched_rows = df_1[df_1.index.isin(mapped_keys)]

        # Return only the requested columns
        if not matched_rows.empty:
            return matched_rows[columns_to_add]
        else:
            return pd.DataFrame([["NA"] * len(columns_to_add)], columns=columns_to_add)

    # Apply the mapping and retrieval logic for each row in df_2
    results = df_2[column_to_map].apply(map_row)

    # Combine results into a single DataFrame
    new_columns_df = pd.concat(results.values).reset_index(drop=True)

    # Add the new columns to df_2
    df_2 = pd.concat([df_2.reset_index(drop=True), new_columns_df.reset_index(drop=True)], axis=1)
    return df_2

# Perform Groupby
def groupby_function(df, groupby_column, weight_column='QTY_kg', sort=True):
    """
    Prepares a DataFrame for analysis by:
    - Retaining necessary columns
    - Removing rows with missing values in the weight column
    - Ensuring proper data types
    - Grouping data by a specified column and summing the weights
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        groupby_column (str): The column to group by.
        weight_column (str): The column containing weights to sum. Default is 'QTY_kg'.
        sort (bool): Whether to sort the grouped DataFrame by weight in descending order.
    
    Returns:
        pd.DataFrame: Grouped and aggregated DataFrame.
    """
    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")
    
    # Validate column names
    if groupby_column not in df.columns:
        raise ValueError(f"'{groupby_column}' column is missing in the DataFrame.")
    if weight_column not in df.columns:
        raise ValueError(f"'{weight_column}' column is missing in the DataFrame.")
    
    # Step 1: Retain only relevant columns
    df = df[[weight_column, groupby_column]].copy()
    
    # Step 2: Remove rows with missing weight values
    df = df.loc[df[weight_column].notna()]
    
    # Step 3: Convert weight column to numeric and handle errors
    df[weight_column] = pd.to_numeric(df[weight_column], errors='coerce')
    df = df.loc[df[weight_column].notna()]  # Remove rows where conversion failed
    
    # Step 4: Ensure groupby_column is treated as string (if applicable)
    df[groupby_column] = df[groupby_column].astype(str).str.strip()
    
    # Step 5: Group by the specified column and sum the weights
    dfg = df.groupby(groupby_column, as_index=False).agg({weight_column: 'sum'})
    
    # Step 6: Sort results if specified
    if sort:
        dfg = dfg.sort_values(by=weight_column, ascending=False)
    
    # Reset index for clean output
    dfg.reset_index(drop=True, inplace=True)
    
    return dfg


# Groupby for Containers
def groupby_function(df, groupby_column, weight_column=None, sort=True, count_only=False):
    """
    Groups a DataFrame by a specified column(s) and either sums weights or counts rows.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        groupby_column (str or list): The column(s) to group by.
        weight_column (str): The column containing weights to sum. Default is None for counting only.
        sort (bool): Whether to sort the grouped DataFrame by the aggregation column in descending order.
        count_only (bool): Whether to count occurrences instead of summing weights. Default is False.
    
    Returns:
        pd.DataFrame: Grouped and aggregated DataFrame.
    """
    # Step 1: Ensure groupby_column is a list
    if isinstance(groupby_column, str):
        groupby_column = [groupby_column]
    
    for col in groupby_column:
        if col not in df.columns:
            raise ValueError(f"'{col}' must exist in the DataFrame.")
    
    # Step 2: If counting only, ignore weight_column
    if count_only:
        dfg_1 = df.groupby(groupby_column).size().reset_index(name='Count')
    else:
        if weight_column not in df.columns:
            raise ValueError(f"'{weight_column}' must exist in the DataFrame.")
        
        # Retain only relevant columns
        df = df[groupby_column + [weight_column]]
        
        # Remove rows with missing weight values
        df = df.loc[df[weight_column].notna()]
        
        # Convert weight column to numeric and handle errors
        df[weight_column] = pd.to_numeric(df[weight_column], errors='coerce')
        df = df.loc[df[weight_column].notna()]  # Remove rows where conversion failed
        
        # Group by the specified columns and sum the weights
        dfg_1 = df.groupby(groupby_column, as_index=False)[weight_column].sum()
    
    # Step 3: Sort results if specified
    if sort:
        sort_column = 'Count' if count_only else weight_column
        dfg_1 = dfg_1.sort_values(by=sort_column, ascending=False)
    
    return dfg_1