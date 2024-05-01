################################################################################
# The data underlying this script is taken from the Perseus Digital Library (https://www.perseus.tufts.edu/hopper/)
# and published under the Creative Commons Attribution-ShareAlike 3.0 US Licence (https://creativecommons.org/licenses/by-sa/3.0/us/)
################################################################################

from flask import Flask, request, render_template
from flask_bootstrap import Bootstrap
import pandas as pd
import re
from collections import Counter
import json

# Create the Flask app as "app" and specify the template folder name
app = Flask(__name__, template_folder='templates')

# Bootstrap-Flask requires this line
Bootstrap(app)

# Import the data
corpus = pd.read_csv('../latin-n-gram-viewer/latin-corpus.csv', encoding='UTF-8')

# Make a copy of the "corpus" dataframe for future separate data manipulation
summary = corpus


# Set route to display the app homepage accessible at '/'
@app.route('/', methods=("POST", "GET"))
def index():
    return render_template('dash.html')


# Set another route to display the results
@app.route('/results', methods=("POST", "GET"))
def search():
    # Set global variables that will be sent to the template 'results.html'
    global json_objects_final, summary_tables
    if request.method == "POST":
        # Get string input saved into variable "ngram-search" in HTML form
        ngram = request.form.get("ngram-search")

        # Split the string input into a list of tokens. The separator is specified within the brackets
        input_string = ngram.split(", ")

        # Remove empty strings and whitespaces from the tokens list, in order not to search them in the next step.
        # The strip() function removes leading and trailing whitespace characters (spaces, tabs, and newlines) from a
        # string. It returns a new string with the whitespace characters removed.
        # The results are collected by means of a list comprehension
        tokens = [t for t in input_string if t.strip()]

        # Collect the raw occurrences within the texts for each ngram in the string input
        def item_counter(tokens_list):
            # Create an empty list
            tokens_count = []
            # For each ngram from the tokens list create a new "Result" series, which represents also a new column
            # in the "corpus" dataframe. The column contains a counter object for each text instance,
            # i.e. a dictionary where the key is represented by the ngram itself and the value corresponds
            # to the ngram raw occurrences within a single text
            for item in tokens_list:
                # Each ngram is retrieved within the text by means of the re.compile() and findall() functions:
                # re.compile() sets a regex pattern corresponding the ngram to be searched; findall() looks for
                # all occurrences of the pattern defined by re.compile() within each text
                corpus['Results'] = [Counter(re.compile(item).findall(text)) for text in corpus['Text']]
                # Normalize those resulting counter objects with zero ngram-occurrences
                for idx, row in corpus['Results'].items():
                    # Check if the counter object is empty
                    if not row:
                        # In that case, update the counter object to "counter({'item': 0})"
                        corpus['Results'].update(pd.Series([Counter({item: 0})], index=[idx]))
                # Save each "Results" series into the "tokens_count" list
                tokens_count.append(corpus['Results'])
            return tokens_count

        # List of token/n-gram counter objects, each one structured as {ngram: count}.
        # The list length for each token/n-gram is equal to the dataset length, in this case 177
        ngram_raw_counts = item_counter(tokens)

        # Sum (and group) the ngram occurrences from each text by historical era
        def sum_occurrences_by_era(counters):
            # Define an empty list
            occurrences_by_era_list = []
            # For each list of ngram counter objects
            for item in counters:
                # Extract the key from each counter object
                key = list(item.values[0])[0]
                # Create a series with each list of ngram counter objects
                corpus['Results'] = item
                # Group the ngram frequencies related to each text in the corpus by historical era,
                # i.e. sum the raw frequencies for texts belonging to the same epoch
                total_per_year = corpus.groupby(by='Period')['Results'].sum()
                # Normalize in case of empty counter objects
                for idx, row in total_per_year.items():
                    # Check if the counter object is empty
                    if not row:
                        # In that case, update the counter object to "counter({'key': 0})"
                        total_per_year.update(pd.Series([Counter({key: 0})], index=[idx]))
                # Save each "total_per_year" series into the "occurrences_by_era_list" list
                occurrences_by_era_list.append(total_per_year)
            return occurrences_by_era_list

        # List of summed ngram raw frequencies by historical era
        total_occurrences_by_era = sum_occurrences_by_era(ngram_raw_counts)

        # Serialize "total_occurrences_by_era" to JSON format and save to a list comprehension
        serialize_json = [e.to_json() for e in total_occurrences_by_era]

        # Parse each JSON string and convert it into a dictionary. Save the results to a list comprehension
        load_json = [json.loads(jsn) for jsn in serialize_json]

        # Format the ngram dictionaries structure before visualization
        def update_json(data):
            # Create an empty list
            output_json = []
            # Update the ngram dictionary structure so that 'id' is the historical era, 'ngram' corresponds to the
            # ngram name, 'frequency' is the integer value corresponding to the total n-gram occurrences within the
            # specific historical epoch.
            # The previous JSON object structure – i.e. {period : {ngram: frequency}} – saved into the 'load_json' list
            # is thus extended to the new properties 'ngram' and 'frequency'
            for key, value in data.items():
                item = {
                    "id": key,
                    "ngram": ''.join(list(value.keys())),
                    "frequency": int('0' + ''.join(map(str, list(value.values()))))
                }
                # Save each updated ngram dictionary into the "output_json" list
                output_json.append(item)
            return output_json

        # Update all ngram JSON dictionaries and save them into a list comprehension
        updated_json_dictionaries = [update_json(dictionary) for dictionary in load_json]

        # Sum all values of the "frequency" property and divide each "frequency" property by this total
        def update_json_frequency(json_data):
            # Create an empty list
            output_json1 = []
            # Compute 'total_frequency' by summing all absolute occurrences in the corpus for each n-gram
            total_frequency = sum(obj['frequency'] for obj in json_data)
            # Extend each ngram JSON object with a new property 'frequencyRelative'
            for obj in json_data:
                # The 'frequencyRelative' property is obtained by dividing the ngram absolute frequency
                # by the 'total_frequency' value
                # Prevent ZeroDivisionError setting the 'frequencyRelative' value to zero, if necessary
                try:
                    obj['frequencyRelative'] = obj['frequency'] / total_frequency
                except ZeroDivisionError:
                    obj['frequencyRelative'] = 0
                # Save each updated ngram JSON object into the "output_json1" list
                output_json1.append(obj)
            return output_json1

        # Apply the update_json_frequency() function to each ngram JSON object and save them into a list comprehension
        json_objects_final = [update_json_frequency(x) for x in updated_json_dictionaries]

        ###
        # Define a function to create distinct subsets of the 'corpus' dataframe, based on unique values of 'Period'.
        # The basis is the "corpus" dataframe (here saved into the 'summary' variable), where the 'Index' and 'Text'
        # columns are dropped and to which the "Results" column, containing all the n-gram counter objects from the
        # dataset, is added

        def create_df_subsets(dataframe, raw_frequency_counters_list):
            # Define a new list
            new = []
            # Iterate over each single n-gram counter object in the dataset, i.e. over each counter object defined for a
            # specific n-gram and related to a specific text within the corpus
            for i in raw_frequency_counters_list:
                # Assign each counter object 'i' to a new series to be appended to the dataframe
                dataframe['Results'] = i
                # Create a dataframe subset including all the entries without zero ngram-occurrences.
                # The str.contains() method is used to check if the column contains the specified pattern. The `~`
                # operator is used to negate the condition, so rows containing the pattern are excluded from the
                # subsets-dataframe.
                var = dataframe[~dataframe['Results'].astype(str).str.contains(': 0}')]

                # Sort the obtained dataframe "var" alphabetically by title, which is the first table field
                # from left to right
                var = var.sort_values('Title')

                # Group the obtained dataframe by the 'Period' column
                grouped = var.groupby('Period')

                # Create subsets-dataframes based on unique values in the 'Period' column
                sub_dataframes = {period: values for period, values in grouped}

                # Accessing subsets-dataframes by period name
                #
                distinct_df = []
                for p, sub_df in sub_dataframes.items():
                    distinct_df.append(sub_df)
                # Save the results to the "new" list
                new.append(distinct_df)
            return new

        # Remove from the 'summary' dataframe the unuseful columns for visualization, i.e. 'Index' and 'Text'
        ngram_df = summary.drop(summary.columns[[0, 3]], axis=1)

        # Apply the 'create_df_subsets()' function
        subsets = create_df_subsets(ngram_df, ngram_raw_counts)

        # List comprehension of corpus subsets converted to the HTML format
        summary_tables = [t.to_html(classes='table table-striped', index=False) for table in subsets for t in table]

    return render_template('results.html', json=json_objects_final, tables=summary_tables)


if __name__ == '__main__':
    app.run()
